mport logging
import json
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOTO_CFG = Config(
    retries={
        "max_attempts": 10,
        "mode": "standard",
    }
)

def parse_event(event):
    # SNS trigger
    if "Records" in event:
        return json.loads(event["Records"][0]["Sns"]["Message"])

    #  EventBridge trigger
    if "detail" in event:
        return event["detail"]

    #  Direct Lambda test
    return event



def assume_role(account_id, role_name, region):
    sts = boto3.client("sts")

    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"

    resp = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName="CMKUpdateSession"
    )

    creds = resp["Credentials"]

    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region
    )


def get_ees_admin_role_arn_update(
    session=None,
    account_id=None,
):
    """
    Discover the exact AWSReservedSSO_EESAdminAccess_* role
    in the target account.

    The ARN returned by IAM is used exactly as returned, including
    the aws-reserved/sso.amazonaws.com path.
    """

    if session is not None:
        iam = session.client(
            "iam",
            config=BOTO_CFG,
        )
    else:
        iam = boto3.client(
            "iam",
            config=BOTO_CFG,
        )

    matching_roles = []
    paginator = iam.get_paginator("list_roles")

    for page in paginator.paginate():
        for role in page.get("Roles", []):
            role_name = role.get("RoleName", "")

            if not role_name.startswith(
                "AWSReservedSSO_EESAdminAccess_"
            ):
                continue

            role_arn = role["Arn"]

            if account_id:
                role_account_id = role_arn.split(":")[4]

                if role_account_id != str(account_id):
                    continue

            matching_roles.append(role_arn)

    if not matching_roles:
        raise RuntimeError(
            "AWSReservedSSO_EESAdminAccess role was not found "
            f"in account {account_id or 'the current account'}"
        )

    if len(matching_roles) > 1:
        raise RuntimeError(
            "Multiple AWSReservedSSO_EESAdminAccess roles were found "
            f"in account {account_id}: {matching_roles}"
        )

    logger.info(
        "Found EES Admin SSO role in account %s: %s",
        account_id or "current account",
        matching_roles[0],
    )

    return matching_roles[0]

def create_kms_key(session, account_id, region):
    kms = session.client("kms")
    sso_role_arn = get_ees_admin_role_arn_update(
        session=session,
        account_id=account_id,
    )

    cckm_role_arn = (
        f"arn:aws:iam::{account_id}:role/CCKM_Execution_Role"
    )

    terraform_role_arn = (
        f"arn:aws:iam::{account_id}:role/"
        "CCKM_Execution_Role"
    )
    key_policy = {
        "Version": "2012-10-17",
        "Statement": [
            # {
            #     "Sid": "EnableRootPermissions",
            #     "Effect": "Allow",
            #     "Principal": {
            #         "AWS": f"arn:aws:iam::{account_id}:root"
            #     },
            #     "Action": "kms:*",
            #     "Resource": "*",
            # },
            {
                "Sid": "EnableRootPermissions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action": [
                    "kms:CancelKeyDeletion",
                    "kms:ConnectCustomKeyStore",
                    "kms:CreateAlias",
                    "kms:CreateCustomKeyStore",
                    "kms:CreateGrant",
                    "kms:CreateKey",
                    "kms:Decrypt",
                    "kms:DeleteAlias",
                    "kms:DeleteCustomKeyStore",
                    "kms:DeleteImportedKeyMaterial",
                    "kms:DescribeCustomKeyStores",
                    "kms:DescribeKey",
                    "kms:DisableKey",
                    "kms:DisableKeyRotation",
                    "kms:DisconnectCustomKeyStore",
                    "kms:EnableKey",
                    "kms:EnableKeyRotation",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:GenerateMac",
                    "kms:GenerateRandom",
                    "kms:GetKeyPolicy",
                    "kms:GetKeyRotationStatus",
                    "kms:GetParametersForImport",
                    "kms:GetPublicKey",
                    "kms:ImportKeyMaterial",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeyPolicies",
                    "kms:ListKeyRotations",
                    "kms:ListKeys",
                    "kms:ListResourceTags",
                    "kms:ListRetirableGrants",
                    "kms:PutKeyPolicy",
                    "kms:ReEncryptFrom",
                    "kms:ReEncryptTo",
                    "kms:ReplicateKey",
                    "kms:RetireGrant",
                    "kms:RevokeGrant",
                    "kms:RotateKeyOnDemand",
                    "kms:ScheduleKeyDeletion",
                    "kms:Sign",
                    "kms:TagResource",
                    "kms:UntagResource",
                    "kms:UpdateAlias",
                    "kms:UpdateCustomKeyStore",
                    "kms:UpdateKeyDescription",
                    "kms:UpdatePrimaryRegion",
                    "kms:Verify",
                    "kms:VerifyMac"
                ],
                "Resource": "*",
            },
            {
                "Sid": "AllowLambdaUse",
                "Effect": "Allow",
                "Principal": {
                    "AWS": (
                        f"arn:aws:iam::{account_id}:role/"
                        "AWS-FIREWALL-CMEK-ROLE"
                    )
                },
                "Action": [
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:DescribeKey",
                ],
                "Resource": "*",
            },
            {
                "Sid": "Enable Access for Key Administrators",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        sso_role_arn,
                        cckm_role_arn,
                    ]
                },
                "Action": [
                    "kms:Disable*",
                    "kms:List*",
                    "kms:Describe*",
                    "kms:Get*",
                    "kms:Update*",
                    "kms:Create*",
                    "kms:Delete*",
                    "kms:Enable*",
                    "kms:Rotate*",
                    "kms:TagResource",
                    "kms:UntagResource",
                    "kms:CancelKeyDeletion",
                    "kms:ImportKeyMaterial",
                    "kms:ScheduleKeyDeletion",
                    "kms:PutKeyPolicy",
                    "kms:ConnectCustomKeyStore",
                    "kms:DisconnectCustomKeyStore",
                ],
                "Resource": "*",
            },
            # {
            #     "Sid": "Enable Terraform Role Permissions",
            #     "Effect": "Allow",
            #     "Principal": {
            #         "AWS": terraform_role_arn
            #     },
            #     "Action": [
            #         "kms:UpdateKeyDescription",
            #         "kms:UpdateAlias",
            #         "kms:UntagResource",
            #         "kms:TagResource",
            #         "kms:ScheduleKeyDeletion",
            #         "kms:ReplicateKey",
            #         "kms:List*",
            #         "kms:GetKeyRotationStatus",
            #         "kms:GetKeyPolicy",
            #         "kms:GenerateDataKey",
            #         "kms:Encrypt",
            #         "kms:EnableKeyRotation",
            #         "kms:EnableKey",
            #         "kms:DisableKey",
            #         "kms:DescribeKey",
            #         "kms:DeleteAlias",
            #         "kms:Decrypt",
            #         "kms:CreateKey",
            #         "kms:CreateAlias",
            #         "kms:CancelKeyDeletion",
            #     ],
            #     "Resource": [
            #         f"arn:aws:kms:*:{account_id}:key/*",
            #         f"arn:aws:kms:*:{account_id}:alias/*",
            #     ],
            # },
            {
                "Sid": "S3 Permissions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action": [
                    "kms:UntagResource",
                    "kms:TagResource",
                    "kms:ReEncrypt*",
                    "kms:ListGrants",
                    "kms:GenerateDataKey*",
                    "kms:Encrypt*",
                    "kms:DescribeKey",
                    "kms:Decrypt",
                ],
                "Resource": [
                    f"arn:aws:kms:*:{account_id}:key/*",
                    f"arn:aws:kms:*:{account_id}:alias/*",
                ],
                "Condition": {
                    "StringEquals": {
                        "kms:ViaService": [
                            f"s3.{region}.amazonaws.com"
                        ]
                    }
                },
            },
            {
                "Sid": "Enable view KMS key details in console",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action": [
                    "kms:List*",
                    "kms:GetKeyRotationStatus",
                    "kms:GetKeyPolicy",
                    "kms:DescribeKey",
                ],
                "Resource": [
                    f"arn:aws:kms:*:{account_id}:key/*",
                    f"arn:aws:kms:*:{account_id}:alias/*",
                ],
            },
            {
                "Sid": "Restrict post-key-creation policy modification",
                "Effect": "Deny",
                "NotPrincipal": {
                    "AWS": [
                        sso_role_arn,
                        cckm_role_arn,
                    ]
                },
                "Action": "kms:PutKeyPolicy",
                "Resource": "*",
                "Condition": {
                    "Bool": {
                        "kms:BypassPolicyLockoutSafetyCheck": "true"
                    }
                },
            },
            {
                "Sid": "Allow attachment of persistent resources",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action": [
                    "kms:RevokeGrant",
                    "kms:ListGrants",
                    "kms:CreateGrant",
                ],
                "Resource": "*",
                "Condition": {
                    "Bool": {
                        "kms:GrantIsForAWSResource": "true"
                    }
                },
            },
            # {
            #     "Sid": "AllowNetworkFirewallUsage",
            #     "Effect": "Allow",
            #     "Principal": "*",
            #     "Action": "kms:*",
            #     "Resource": "*",
            #     "Condition": {
            #         "Bool": {
            #             "kms:GrantIsForAWSResource": "true"
            #         },
            #         "StringEquals": {
            #             "kms:ViaService": (
            #                 f"network-firewall.{region}.amazonaws.com"
            #             ),
            #             "kms:CallerAccount": str(account_id),
            #         },
            #     },
            # },
            {
                "Sid": "AllowNetworkFirewallUsage",
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    "kms:CancelKeyDeletion",
                    "kms:ConnectCustomKeyStore",
                    "kms:CreateAlias",
                    "kms:CreateCustomKeyStore",
                    "kms:CreateGrant",
                    "kms:CreateKey",
                    "kms:Decrypt",
                    "kms:DeleteAlias",
                    "kms:DeleteCustomKeyStore",
                    "kms:DeleteImportedKeyMaterial",
                    "kms:DescribeCustomKeyStores",
                    "kms:DescribeKey",
                    "kms:DisableKey",
                    "kms:DisableKeyRotation",
                    "kms:DisconnectCustomKeyStore",
                    "kms:EnableKey",
                    "kms:EnableKeyRotation",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:GenerateMac",
                    "kms:GenerateRandom",
                    "kms:GetKeyPolicy",
                    "kms:GetKeyRotationStatus",
                    "kms:GetParametersForImport",
                    "kms:GetPublicKey",
                    "kms:ImportKeyMaterial",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeyPolicies",
                    "kms:ListKeyRotations",
                    "kms:ListKeys",
                    "kms:ListResourceTags",
                    "kms:ListRetirableGrants",
                    "kms:PutKeyPolicy",
                    "kms:ReEncryptFrom",
                    "kms:ReEncryptTo",
                    "kms:ReplicateKey",
                    "kms:RetireGrant",
                    "kms:RevokeGrant",
                    "kms:RotateKeyOnDemand",
                    "kms:ScheduleKeyDeletion",
                    "kms:Sign",
                    "kms:TagResource",
                    "kms:UntagResource",
                    "kms:UpdateAlias",
                    "kms:UpdateCustomKeyStore",
                    "kms:UpdateKeyDescription",
                    "kms:UpdatePrimaryRegion",
                    "kms:Verify",
                    "kms:VerifyMac"
                ],
                "Resource": "*",
                "Condition": {
                    "Bool": {
                        "kms:GrantIsForAWSResource": "true"
                    },
                    "StringEquals": {
                        "kms:ViaService": (
                            f"network-firewall.{region}.amazonaws.com"
                        ),
                        "kms:CallerAccount": str(account_id),
                    },
                },
            },
        ],
    }

    response = kms.create_key(
        Description="Network Firewall CMK",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
        Policy=json.dumps(key_policy)
    )

    key_id = response["KeyMetadata"]["KeyId"]
    key_arn = response["KeyMetadata"]["Arn"]

    kms.create_alias(
        AliasName="alias/aws-network-firewall-v5",
        TargetKeyId=key_id
    )

    return key_arn


def find_firewall_by_vpc(session, vpc_id):
    nf = session.client("network-firewall")

    firewalls = nf.list_firewalls()["Firewalls"]

    matched = []

    for fw in firewalls:
        desc = nf.describe_firewall(
            FirewallName=fw["FirewallName"]
        )

        fw_vpc = desc["Firewall"]["VpcId"]

        if fw_vpc == vpc_id:
            matched.append(desc)

    return matched


def update_firewall(session, firewall_desc, kms_key):
    print(kms_key)
    nf = session.client("network-firewall")

    fw_arn = firewall_desc["Firewall"]["FirewallArn"]

    print(f"Updating firewall CMK: {fw_arn}")

    nf.update_firewall_encryption_configuration(
        FirewallArn=fw_arn,
        EncryptionConfiguration={
            "Type": "CUSTOMER_KMS",
            "KeyId": kms_key
        }
    )


def update_policy(session, policy_arn, kms_key):
    nf = session.client("network-firewall")

    policy_name = policy_arn.split("/")[-1]


    desc = nf.describe_firewall_policy(
        FirewallPolicyName=policy_name
    )

    firewall_policy = desc["FirewallPolicy"]
    update_token = desc["UpdateToken"]

    print(f"Updating firewall policy CMK: {policy_name}")


    nf.update_firewall_policy(
        FirewallPolicyName=policy_name,
        FirewallPolicy=firewall_policy,  
        UpdateToken=update_token,
        EncryptionConfiguration={
            "Type": "CUSTOMER_KMS",
            "KeyId": kms_key
        }
    )

    return firewall_policy



def update_rule_groups(session, policy, kms_key):
    nf = session.client("network-firewall")

    rg_refs = []
    rg_refs.extend(policy.get("StatefulRuleGroupReferences", []))
    rg_refs.extend(policy.get("StatelessRuleGroupReferences", []))

    for rg in rg_refs:
        arn = rg["ResourceArn"]
        name = arn.split("/")[-1]
        account_number = arn.split(':')[4]

        type_ = "STATEFUL" if "stateful-rulegroup" in arn else "STATELESS"

        if account_number == 'aws-managed':
            print(f'Skipping AWS Managed Rule Group: {arn}')
            continue

        print(f'ARN: {arn}')
        print(f'NAME: {name}')
        print(f'TYPE: {type_}')

        try:
            desc = nf.describe_rule_group(
                RuleGroupName=name,
                Type=type_
            )
        except nf.exceptions.ResourceNotFoundException as error:
            print(f'Skipping Firewall Managed Rule Group: {arn}')
            continue

        update_token = desc["UpdateToken"]

        print(f"Updating rule group CMK: {name}")

        kwargs = {
            "RuleGroupName": name,
            "Type": type_,
            "UpdateToken": update_token,
            "EncryptionConfiguration": {
                "Type": "CUSTOMER_KMS",
                "KeyId": kms_key
            }
        }


        if "RuleGroup" in desc and desc["RuleGroup"]:
            kwargs["RuleGroup"] = desc["RuleGroup"]

        elif "Rules" in desc and desc["Rules"]:
            kwargs["Rules"] = desc["Rules"]

        else:
            print(f"No Rules or RuleGroup found for {name}, skipping")
            continue

        try:
            nf.update_rule_group(**kwargs)
        except ClientError as error:
            print(f"Failed to update rule group {name}: {error}")
            continue

#########################################################
# Tagging
#########################################################

def copy_vpc_business_tags(session, account_id, vpc_id, kms_key_arn, nf_resource_arns, iam_role_name=None):
    required_tag_keys = [
        'elvh-infra-env',
        'elvh-workspace',
        'elvh-created-by',
        'elvh-app-servicenow-group',
        'elvh-apm-id',
        'elvh-app-support-dl'
    ]
    print('COPY_TAGS: start vpc_id={} account_id={} kms_key_arn={} nf_targets={}'.format(
        vpc_id, account_id, kms_key_arn, nf_resource_arns))
    try:
        source_tags = {}
        try:
            ec2 = session.client('ec2')
            response = ec2.describe_tags(Filters=[{'Name': 'resource-id', 'Values': [vpc_id]}])
            for tag in response.get('Tags', []):
                source_tags[tag['Key']] = tag['Value']
            print('COPY_TAGS: source VPC tags={}'.format(source_tags))
        except Exception:
            import traceback
            print('COPY_TAGS_ERROR: failed reading tags from vpc {}: {}'.format(vpc_id, traceback.format_exc()))
            return

        tags_to_apply = {}
        missing_keys = []
        for key in required_tag_keys:
            value = source_tags.get(key)
            if value is not None and len(str(value).strip()) > 0:
                tags_to_apply[key] = value
            else:
                missing_keys.append(key)

        if len(missing_keys) > 0:
            print('COPY_TAGS: vpc {} is missing required tags {} - skipping tag copy.'.format(vpc_id, missing_keys))
            return

        print('COPY_TAGS: all required tags present on vpc {} - tags_to_apply={}'.format(vpc_id, tags_to_apply))

        if kms_key_arn:
            print('COPY_TAGS: processing KMS key {}'.format(kms_key_arn))
            try:
                kms = session.client('kms')
                existing_kms_keys = set()
                try:
                    existing = kms.list_resource_tags(KeyId=kms_key_arn)
                    for tag in existing.get('Tags', []):
                        existing_kms_keys.add(tag['TagKey'])
                    print('COPY_TAGS: KMS key {} existing tag keys={}'.format(kms_key_arn, existing_kms_keys))
                except Exception:
                    import traceback
                    print('COPY_TAGS_ERROR: list_resource_tags failed for {}: {}'.format(kms_key_arn, traceback.format_exc()))
                for tag_key in tags_to_apply.keys():
                    try:
                        if tag_key in existing_kms_keys:
                            print('COPY_TAGS: tag {} already exists on KMS key {} - skipping.'.format(tag_key, kms_key_arn))
                            continue
                        kms.tag_resource(KeyId=kms_key_arn, Tags=[{'TagKey': tag_key, 'TagValue': tags_to_apply[tag_key]}])
                        print('COPY_TAGS: added tag {}={} to KMS key {}'.format(tag_key, tags_to_apply[tag_key], kms_key_arn))
                    except Exception:
                        import traceback
                        print('COPY_TAGS_ERROR: failed adding tag {} to KMS key {}: {}'.format(tag_key, kms_key_arn, traceback.format_exc()))
                        continue
            except Exception:
                import traceback
                print('COPY_TAGS_ERROR: KMS tagging block failed for {}: {}'.format(kms_key_arn, traceback.format_exc()))

        #########################################################
        # IAM Role Tagging
        #########################################################

        if iam_role_name:
            print('COPY_TAGS: processing IAM role {}'.format(iam_role_name))

            try:
                iam = session.client('iam')

                existing_iam_keys = set()

                try:
                    response = iam.list_role_tags(
                        RoleName=iam_role_name
                    )

                    for tag in response.get('Tags', []):
                        existing_iam_keys.add(tag['Key'])

                    print(
                        'COPY_TAGS: IAM role {} existing tag keys={}'.format(
                            iam_role_name,
                            existing_iam_keys
                        )
                    )

                except Exception:
                    import traceback
                    print(
                        'COPY_TAGS_ERROR: list_role_tags failed for {}: {}'.format(
                            iam_role_name,
                            traceback.format_exc()
                        )
                    )

                for tag_key in tags_to_apply.keys():

                    try:

                        if tag_key in existing_iam_keys:
                            print(
                                'COPY_TAGS: tag {} already exists on IAM role {} - skipping.'.format(
                                    tag_key,
                                    iam_role_name
                                )
                            )
                            continue

                        iam.tag_role(
                            RoleName=iam_role_name,
                            Tags=[
                                {
                                    'Key': tag_key,
                                    'Value': tags_to_apply[tag_key]
                                }
                            ]
                        )

                        print(
                            'COPY_TAGS: added tag {}={} to IAM role {}'.format(
                                tag_key,
                                tags_to_apply[tag_key],
                                iam_role_name
                            )
                        )

                    except Exception:
                        import traceback
                        print(
                            'COPY_TAGS_ERROR: failed adding tag {} to IAM role {}: {}'.format(
                                tag_key,
                                iam_role_name,
                                traceback.format_exc()
                            )
                        )

            except Exception:
                import traceback
                print(
                    'COPY_TAGS_ERROR: IAM tagging block failed for {}: {}'.format(
                        iam_role_name,
                        traceback.format_exc()
                    )
                )
        if nf_resource_arns:
            try:
                nf = session.client('network-firewall')
            except Exception:
                import traceback
                print('COPY_TAGS_ERROR: could not create network-firewall client: {}'.format(traceback.format_exc()))
                nf = None
            seen = set()
            for resource_arn in nf_resource_arns:
                if not resource_arn or resource_arn in seen:
                    continue
                seen.add(resource_arn)
                try:
                    arn_account = resource_arn.split(':')[4]
                except Exception:
                    arn_account = None
                if arn_account is not None and arn_account != str(account_id):
                    print('COPY_TAGS: resource {} not owned by vended account {} (owner={}) - skipping.'.format(
                        resource_arn, account_id, arn_account))
                    continue
                if nf is None:
                    continue
                print('COPY_TAGS: processing network-firewall resource {}'.format(resource_arn))
                existing_nf_keys = set()
                try:
                    existing = nf.list_tags_for_resource(ResourceArn=resource_arn)
                    for tag in existing.get('Tags', []):
                        existing_nf_keys.add(tag['Key'])
                    print('COPY_TAGS: resource {} existing tag keys={}'.format(resource_arn, existing_nf_keys))
                except Exception:
                    import traceback
                    print('COPY_TAGS_ERROR: list_tags_for_resource failed for {}: {}'.format(resource_arn, traceback.format_exc()))
                for tag_key in tags_to_apply.keys():
                    try:
                        if tag_key in existing_nf_keys:
                            print('COPY_TAGS: tag {} already exists on {} - skipping.'.format(tag_key, resource_arn))
                            continue
                        nf.tag_resource(ResourceArn=resource_arn, Tags=[{'Key': tag_key, 'Value': tags_to_apply[tag_key]}])
                        print('COPY_TAGS: added tag {}={} to {}'.format(tag_key, tags_to_apply[tag_key], resource_arn))
                    except Exception:
                        import traceback
                        print('COPY_TAGS_ERROR: failed adding tag {} to {}: {}'.format(tag_key, resource_arn, traceback.format_exc()))
                        continue

        print('COPY_TAGS: completed vpc_id={}'.format(vpc_id))
    except Exception:
        import traceback
        print('COPY_TAGS_FATAL: unexpected error: {}'.format(traceback.format_exc()))

def lambda_handler(event, context):

    message = parse_event(event)

    account_id = message["VendedAccountId"]
    role_name = os.environ["AssumeRoleName"]
    region = message["Region"]
    vpc_id = message["VpcId"]

    session = assume_role(account_id, role_name, region)

    kms_key = create_kms_key(session, account_id, region)

    firewalls = find_firewall_by_vpc(session, vpc_id)

    if not firewalls:
        print(f"No firewall found for VPC: {vpc_id}")
        return

    tagging_targets = []
    for fw in firewalls:
        update_firewall(session, fw, kms_key)

        policy_arn = fw["Firewall"]["FirewallPolicyArn"]

        policy = update_policy(session, policy_arn, kms_key)

        update_rule_groups(session, policy, kms_key)

        tagging_targets.append(fw["Firewall"]["FirewallArn"])
        tagging_targets.append(policy_arn)
        for rg in policy.get("StatefulRuleGroupReferences", []) + policy.get("StatelessRuleGroupReferences", []):
            tagging_targets.append(rg["ResourceArn"])

    copy_vpc_business_tags(
    session,
    account_id,
    vpc_id,
    kms_key,
    tagging_targets,
    "AWS-FIREWALL-CMEK-ROLE"
    )

    return {"status": "completed"}