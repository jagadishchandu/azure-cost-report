"""
Activates all of your eligible Microsoft Entra ID PIM roles by driving the real
Entra admin center UI in a browser you log into yourself (Playwright), using one
justification message for every role.

This does NOT call the Microsoft Graph API directly - it automates the same
clicks you'd make by hand in https://entra.microsoft.com, so it works even when
no Graph API consent has been granted for scripting tools, since the Entra portal
itself already has its own sanctioned access. You still sign in (and complete MFA)
yourself, in a real, visible browser window.

Setup (one time):
    pip install playwright
    playwright install chromium

Usage:
    python activate_pim_roles_ui.py --justification "Performing scheduled maintenance"

    # Preview only - fills the reason field but does not click the final Activate button
    python activate_pim_roles_ui.py --justification "Test run" --dry-run

Notes:
    - The first time you run this, log in (and complete MFA) in the browser window
      when prompted, then press Enter in the terminal to continue. A persistent
      browser profile (--profile-dir) is used so you typically won't need to log
      in again on later runs.
    - The Entra admin center is a Fluent UI single-page app with virtualized lists
      and side panels; exact element text/roles can differ slightly by tenant
      configuration or after a Microsoft UI update. If a step fails to find an
      element, run `playwright codegen https://entra.microsoft.com`, perform that
      one step manually, and copy the generated locator into this script.
    - If a role's PIM policy requires approval, activation submits a pending
      request instead of activating immediately - that's expected and the script
      will report it as "submitted", not "activated".
"""

import argparse
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

PIM_MY_ROLES_HINT_URL = "https://entra.microsoft.com/"
DEFAULT_PROFILE_DIR = str(Path(__file__).parent / ".pim_browser_profile")


def wait_for_manual_login(page):
    print("\nA browser window has opened.")
    print("If you are not already signed in, sign in now (complete MFA if prompted).")
    input("Once you can see the Entra admin center home page, press Enter here to continue...")


def navigate_to_pim_my_roles(page):
    print("Navigating to Privileged Identity Management > My roles...")
    page.goto(PIM_MY_ROLES_HINT_URL, wait_until="domcontentloaded")

    # Use the portal's global search box rather than a deep link, since deep-link
    # blade IDs change more often than the visible search UX.
    search_box = page.get_by_role("searchbox").first
    search_box.click()
    search_box.fill("Privileged Identity Management")
    page.keyboard.press("Enter")

    result = page.get_by_text("Privileged Identity Management", exact=False).first
    result.click()

    my_roles_link = page.get_by_role("link", name="My roles").first
    my_roles_link.click()

    entra_roles_tab = page.get_by_role("tab", name="Microsoft Entra roles").first
    entra_roles_tab.click()

    eligible_tab = page.get_by_role("tab", name="Eligible assignments").first
    eligible_tab.click()

    page.wait_for_load_state("networkidle")


def scroll_and_collect_role_rows(page, max_scrolls=20):
    """Scroll the (likely virtualized) list so all eligible role rows get rendered,
    then return the distinct role display names found."""
    role_names = set()
    grid = page.get_by_role("grid").first

    for _ in range(max_scrolls):
        rows = grid.get_by_role("row")
        count = rows.count()
        for i in range(count):
            row = rows.nth(i)
            cells = row.get_by_role("gridcell")
            if cells.count() > 0:
                text = cells.first.inner_text().strip()
                if text:
                    role_names.add(text)
        grid.hover()
        page.mouse.wheel(0, 800)
        time.sleep(0.5)

    return sorted(role_names)


def activate_role(page, role_name, justification, dry_run):
    print(f"\nActivating '{role_name}'...")

    row = page.get_by_role("row", name=role_name).first
    try:
        row.get_by_role("button", name="Activate").click(timeout=5000)
    except PlaywrightTimeoutError:
        row.get_by_role("link", name="Activate").click(timeout=5000)

    reason_box = page.get_by_label("Reason", exact=False).first
    reason_box.click()
    reason_box.fill(justification)

    if dry_run:
        print(f"  [dry-run] Reason filled, NOT submitting activation for '{role_name}'.")
        cancel_button = page.get_by_role("button", name="Cancel").first
        if cancel_button.is_visible():
            cancel_button.click()
        return "dry-run"

    activate_button = page.get_by_role("button", name="Activate", exact=True).last
    activate_button.click()

    try:
        page.get_by_text("succeeded", exact=False).first.wait_for(timeout=15000)
        print(f"  Activated '{role_name}'.")
        return "activated"
    except PlaywrightTimeoutError:
        try:
            page.get_by_text("pending", exact=False).first.wait_for(timeout=5000)
            print(f"  Submitted '{role_name}' for approval (pending).")
            return "pending"
        except PlaywrightTimeoutError:
            print(f"  Could not confirm outcome for '{role_name}' - check the portal manually.")
            return "unknown"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--justification", required=True, help="Justification message used for every role activation")
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE_DIR, help="Persistent browser profile directory (keeps you signed in between runs)")
    parser.add_argument("--dry-run", action="store_true", help="Fill the reason field but do not submit the activation")
    args = parser.parse_args()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            args.profile_dir,
            headless=False,
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()
        page.goto(PIM_MY_ROLES_HINT_URL, wait_until="domcontentloaded")

        wait_for_manual_login(page)
        navigate_to_pim_my_roles(page)

        print("Collecting eligible roles (scrolling to load the full list)...")
        role_names = scroll_and_collect_role_rows(page)

        if not role_names:
            print("No eligible roles found - or the list layout didn't match expected selectors.")
            print("Try `playwright codegen https://entra.microsoft.com` to inspect the live page.")
            context.close()
            sys.exit(1)

        print(f"Found {len(role_names)} eligible role(s):")
        for name in role_names:
            print(f"  - {name}")

        results = {}
        for name in role_names:
            try:
                results[name] = activate_role(page, name, args.justification, args.dry_run)
            except Exception as exc:
                print(f"  Failed to activate '{name}': {exc}")
                results[name] = f"error: {exc}"
            # Return to the eligible assignments list before the next role
            navigate_to_pim_my_roles(page)

        print("\nSummary:")
        for name, outcome in results.items():
            print(f"  {name}: {outcome}")

        context.close()


if __name__ == "__main__":
    main()
