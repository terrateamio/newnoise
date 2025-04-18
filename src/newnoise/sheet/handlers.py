from . import attributes as a
from . import data, matchers
from . import transforms as t


def process(
    row, product_ms, pricing_ms, priced_by, service_provider, tf_resource, service_class
):
    return data.process(
        row,
        a.with_({"type": a.const(tf_resource)}, product_ms),
        a.with_(
            {
                "end_provision_amount": a.product(
                    "maxVolumeSize", t=t.normalize_provision
                ),
                "end_usage_amount": a.price("endUsageAmount"),
                "purchase_option": a.price(
                    "purchaseOption", t=t.normalize_purchase_option
                ),
                "region": a.product("regionCode"),
                "service_class": service_class,
                "service_provider": a.const(service_provider),
                "start_provision_amount": a.product(
                    "minVolumeSize", t=t.normalize_provision
                ),
                "start_usage_amount": a.price("startUsageAmount"),
            },
            pricing_ms,
        ),
        priced_by,
    )


class BaseHandler:
    SERVICE_PROVIDER = "overwrite with service provider"
    TF = "overwrite with name of TF resource"

    def __init__(self, **match_params):
        self.match_params = match_params

    def match(self, row):
        return True

    def match_currency(self, row, ccy=None):
        return matchers.price_currency(row, ccy=ccy)

    def process(self, row):
        raise NotImplementedError()


class AWSBaseHandler(BaseHandler):
    SERVICE_PROVIDER = "aws"


class BaseInstanceHandler(AWSBaseHandler):
    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.required_attrs(row, ["instanceType"])
        )

    def process(self, row):
        return process(
            row,
            {
                "values.instance_type": a.product("instanceType"),
            },
            {
                "purchase_option": a.price("purchaseOption"),
                "os": a.product("operatingSystem", t=str.lower),
            },
            a.priced_by_time,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("instance"),
        )


class EC2InstanceHandler(BaseInstanceHandler):
    TF = "aws_instance"

    def match(self, row):
        # Only match Linux and Windows instance
        return (
            super().match(row)
            and (
                matchers.product_operation(row, v="RunInstances")
                # Windows
                or matchers.product_operation(row, v="RunInstances:0002")
            )
            and matchers.product_usagetype(
                row, p="UnusedBox", t=t.mk_clean_fun(p="-", s=":")
            )
            and matchers.price_purchaseoption(row, v="on_demand")
        )


class EC2HostHandler(BaseInstanceHandler):
    TF = "aws_ec2_host"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_operation(row, v="RunInstances")
            and matchers.product_usagetype(
                row, p="UnusedDed", t=t.mk_clean_fun(p="-", s=":")
            )
            and matchers.price_purchaseoption(row, v="on_demand")
        )


class LoadBalancerHandler(AWSBaseHandler):
    TF = "aws_lb"

    KEY_LBT = "load_balancer_type"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AWSELB")
            and matchers.product_usagetype(row, v="LoadBalancerUsage")
            and matchers.price_purchaseoption(row, v="on_demand")
        )

    def process(self, row):
        return process(
            row,
            {
                self.KEY_LBT: a.product("operation", t=self.t_operation),
            },
            {},
            a.priced_by_data,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("data"),
        )

    def t_operation(self, attr):
        match attr:
            case "LoadBalancing":
                return "classic"
            case "LoadBalancing:Application":
                return "application"
            case "LoadBalancing:Network":
                return "network"
            case "LoadBalancing:Gateway":
                return "gateway"
        return attr


class RDSBaseHandler(AWSBaseHandler):
    """
    Mixin to provide RDS specific features to each RDS product
    """

    # https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_CreateDBInstance.html
    ENGINE_LOOKUP = {
        "Any": None,
        "Aurora MySQL": "aurora-mysql",
        "Aurora PostgreSQL": "aurora-postgresql",
        "Db2": "db2",
        "MariaDB": "mariadb",
        "MySQL (on-premise for Outpost)": "mysql",
        "MySQL": "mysql",
        "Oracle": "oracle",
        "PostgreSQL (on-premise for Outpost)": "postgres",
        "PostgreSQL": "postgres",
        "SQL Server (on-premise for Outpost)": "sqlserver",
        "SQL Server": "sqlserver",
    }

    EDITION_LOOKUP = {
        None: None,
        "Advanced": "ae",
        "Enterprise": "ee",
        "Standard": "se",
        "Standard One": "se",
        "Standard Two": "se2",
        "Express": "ex",
        "Web": "web",
        "Developer": "dev",
    }

    def a_engine(self, row, price_attrs):
        engine = self.ENGINE_LOOKUP[a.product("databaseEngine")(row, price_attrs)]
        edition = self.EDITION_LOOKUP[a.product("databaseEdition")(row, price_attrs)]
        if edition:
            engine = "-".join([engine, edition])
        return engine

    def t_license_model(self, lm):
        match lm:
            case "Bring your own license":
                return "bring-your-own-license"
            case "License included":
                return "license-included"
            case _:
                return lm

    def t_deployment_option(self, do):
        match do:
            case "Single-AZ":
                return "false"
            case "Multi-AZ":
                return "true"


class RDSInstanceHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and not matchers.product_attr(row, "databaseEdition", c="BYOM")
            and (
                matchers.product_usagetype(row, v="InstanceUsage")
                or matchers.product_usagetype(row, c="InstanceUsage:")
            )
        )

    def process(self, row):
        return process(
            row,
            {
                "values.engine": self.a_engine,
                "values.instance_class": a.product("instanceType"),
                "values.multi_az": a.product(
                    "deploymentOption", t=self.t_deployment_option
                ),
                # 'values.license_model': a.product('licenseModel', t=self.t_license_model)
            },
            {},
            a.priced_by_time,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("instance"),
        )


class RDSIOPSHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and not matchers.product_attr(row, "databaseEdition", c="BYOM")
            and (
                (
                    matchers.product_usagetype(row, s="StorageIOUsage")
                    and matchers.product_attr(row, "volumeType", v="Magnetic")
                )
                or matchers.product_usagetype(row, s="GP3-PIOPS")
                or matchers.product_usagetype(row, s="Multi-AZ-GP3-PIOPS")
                or matchers.product_usagetype(row, s="RDS:PIOPS")
                or matchers.product_usagetype(row, s="RDS:Multi-AZ-PIOPS")
                or matchers.product_usagetype(row, s="RDS:IO2-PIOPS")
                or matchers.product_usagetype(row, s="RDS:Multi-AZ-IO2-PIOPS")
            )
        )

    def process(self, row):
        return process(
            row,
            {
                "values.engine": self.a_engine,
                "values.storage_type": a.product("usagetype", t=self.storage_type),
            },
            {},
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("iops"),
        )

    def storage_type(self, usagetype):
        if usagetype.endswith("StorageIOUsage"):
            return "standard"
        elif usagetype.endswith("GP3-PIOPS") or usagetype.endswith(
            "Multi-AZ-GP3-PIOPS"
        ):
            return "gp3"
        elif usagetype.endswith("RDS:PIOPS") or usagetype.endswith(
            "RDS:Multi-AZ-PIOPS"
        ):
            return "io1"
        elif usagetype.endswith("RDS:IO2-PIOPS") or usagetype.endswith(
            "RDS:Multi-AZ-IO2-PIOPS"
        ):
            return "io2"
        else:
            raise Exception("Invalid storage_type: {}".format(a.attr))


class RDSStorageHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and not matchers.product_attr(row, "databaseEdition", c="BYOM")
            and not matchers.product_attr(row, "databaseEngine", v="Any")
            and (
                matchers.product_usagetype(row, s="StorageUsage")
                or matchers.product_usagetype(row, s="GP2-Storage")
                or matchers.product_usagetype(row, s="GP3-Storage")
                or matchers.product_usagetype(row, s="PIOPS-Storage")
                or matchers.product_usagetype(row, s="PIOPS-Storage-IO2")
            )
            and not matchers.product_usagetype(row, c="Mirror")
            and not matchers.product_usagetype(row, c="Cluster")
        )

    def process(self, row):
        return process(
            row,
            {
                "values.engine": self.a_engine,
                "values.multi_az": a.product(
                    "deploymentOption", t=self.t_deployment_option
                ),
                # 'values.license_model': a.product('licenseModel', t=self.t_license_model)
                "values.storage_type": a.product("usagetype", t=self.storage_type),
            },
            {
                # Start and end usage added automatically, so turn those off.
                "start_usage_amount": a.const(None),
                "end_usage_amount": a.const(None),
            },
            a.priced_by_data,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("storage"),
        )

    def storage_type(self, usagetype):
        if usagetype.endswith("StorageUsage"):
            return "standard"
        elif usagetype.endswith("GP2-Storage"):
            return "gp2"
        elif usagetype.endswith("GP3-Storage"):
            return "gp3"
        elif usagetype.endswith("PIOPS-Storage"):
            return "io1"
        elif usagetype.endswith("PIOPS-Storage-IO2"):
            return "io2"
        else:
            raise Exception("Invalid storage_type: {}".format(usagetype))


class S3OperationsHandler(AWSBaseHandler):
    TF = "aws_s3_bucket"

    def match(self, row):
        return matchers.product_servicecode(row, v="AmazonS3") and (
            matchers.product_group(row, v="S3-API-Tier1")
            or matchers.product_group(row, v="S3-API-Tier2")
        )

    def process(self, row):
        return process(
            row,
            {},
            {
                "tier": a.product("group", t=self.t_tier),
            },
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("requests"),
        )

    def t_tier(self, attr):
        match attr:
            case "S3-API-Tier1":
                return "1"
            case "S3-API-Tier2":
                return "2"
        return attr


class S3StorageHandler(AWSBaseHandler):
    TF = "aws_s3_bucket"

    def match(self, row):
        return matchers.product_servicecode(
            row, v="AmazonS3"
        ) and matchers.product_usagetype(row, c="-TimedStorage-")

    def process(self, row):
        return process(
            row,
            {},
            {"storage_class": a.product("storageClass", t=self.storage_class)},
            a.priced_by_data,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("storage"),
        )

    def storage_class(self, attr):
        return attr.lower().replace(" ", "_").replace("-", "_")


class SQSFIFOHandler(AWSBaseHandler):
    TF = "aws_sqs_queue"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AWSQueueService")
            and matchers.product_usagetype(row, c="Requests")
            and matchers.product_usagetype(row, c="Requests-FIFO")
        )

    def process(self, row):
        return process(
            row,
            {
                "values.fifo_queue": a.const("false"),
            },
            {},
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("requests"),
        )


class SQSHandler(AWSBaseHandler):
    TF = "aws_sqs_queue"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AWSQueueService")
            and matchers.product_usagetype(row, c="Requests")
            and not matchers.product_usagetype(row, c="Requests-FIFO")
        )

    def process(self, row):
        return process(
            row,
            {
                "values.fifo_queue": a.const("true"),
            },
            {},
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("requests"),
        )


class LambdaHandler(AWSBaseHandler):
    TF = "aws_lambda_function"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AWSLambda")
            and (
                matchers.product_usagetype(row, s="Lambda-GB-Second")
                or matchers.product_usagetype(row, s="Lambda-GB-Second-ARM")
                or matchers.product_usagetype(row, s="Lambda-Provisioned-Concurrency")
                or matchers.product_usagetype(
                    row, s="Lambda-Provisioned-Concurrency-ARM"
                )
                or matchers.product_usagetype(row, s="Lambda-Provisioned-GB-Second")
                or matchers.product_usagetype(row, s="Lambda-Provisioned-GB-Second-ARM")
                or (
                    matchers.product_usagetype(row, c="Request")
                    and not matchers.product_usagetype(row, c="Edge")
                )
            )
        )

    def process(self, row):
        return process(
            row,
            {
                "values.architectures": a.product("usagetype", t=self.architectures),
            },
            {
                "arch": a.product("usagetype", t=self.arch),
            },
            a.priced_by(
                {
                    "Lambda-GB-Second": "t",
                    "Request": "o",
                    "Requests": "o",
                }
            ),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.product("usagetype", t=self.service_class),
        )

    def service_class(self, usagetype):
        if usagetype.endswith("Lambda-GB-Second") or usagetype.endswith(
            "Lambda-GB-Second-ARM"
        ):
            return "duration"
        elif "Request" in usagetype and "Edge" not in usagetype:
            return "requests"
        elif usagetype.endswith("Lambda-Provisioned-Concurrency") or usagetype.endswith(
            "Lambda-Provisioned-Concurrency-ARM"
        ):
            return "provisioned_concurrency"
        elif usagetype.endswith("Lambda-Provisioned-GB-Second") or usagetype.endswith(
            "Lambda-Provisioned-GB-Second-ARM"
        ):
            return "provisioned_duration"
        else:
            raise Exception("Unknown usagetype: {}".format(usagetype))

    def arch(self, usagetype):
        if usagetype.endswith("ARM"):
            return "arm64"
        else:
            return "x86"

    def architectures(self, usagetype):
        if usagetype.endswith("ARM"):
            return "arm64"
        else:
            return None


class EBSStorageHandler(AWSBaseHandler):
    TF = "aws_ebs_volume"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.product_usagetype(row, c="VolumeUsage")
        )

    def process(self, row):
        return process(
            row,
            {"values.type": a.product("volumeApiName")},
            {
                # Start and end usage added automatically, so turn those off.
                "start_usage_amount": a.const(None),
                "end_usage_amount": a.const(None),
            },
            a.priced_by({"gb-mo": "a=values.size"}),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("storage"),
        )


class EBSIOPSHandler(AWSBaseHandler):
    TF = "aws_ebs_volume"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.product_usagetype(row, c="IOPS")
            # io2 has special pricing tiers that we need to create in other handlers
            and not matchers.product_attr(row, "volumeApiName", v="io2")
        )

    def process(self, row):
        return process(
            row,
            {"values.type": a.product("volumeApiName")},
            {
                "start_provision_amount": a.product(
                    "volumeApiName", t=self.start_provision_amount
                ),
                "end_provision_amount": a.product(
                    "volumeApiName", t=self.end_provision_amount
                ),
                # Start and end usage added automatically, so turn those off.
                "start_usage_amount": a.const(None),
                "end_usage_amount": a.const(None),
            },
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("iops"),
        )

    def start_provision_amount(self, volume_type):
        if volume_type == "gp3":
            return "3000"
        else:
            return None

    def end_provision_amount(self, volume_type):
        if volume_type == "gp3":
            return "Inf"
        else:
            return None


class EBSIOPSIO2Tier1Handler(AWSBaseHandler):
    TF = "aws_ebs_volume"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.product_usagetype(row, c="IOPS")
            # io2 has special pricing tiers that we need to create in other handlers
            and matchers.product_attr(row, "volumeApiName", v="io2")
            and matchers.product_usagetype(row, c="tier1")
        )

    def process(self, row):
        return process(
            row,
            {"values.type": a.product("volumeApiName")},
            {
                "start_provision_amount": a.const("0"),
                "end_provision_amount": a.const("32000"),
                # Start and end usage added automatically, so turn those off.
                "start_usage_amount": a.const(None),
                "end_usage_amount": a.const(None),
            },
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("iops"),
        )


class EBSIOPSIO2Tier2Handler(AWSBaseHandler):
    TF = "aws_ebs_volume"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.product_usagetype(row, c="IOPS")
            # io2 has special pricing tiers that we need to create in other handlers
            and matchers.product_attr(row, "volumeApiName", v="io2")
            and matchers.product_usagetype(row, c="tier2")
        )

    def process(self, row):
        return process(
            row,
            {"values.type": a.product("volumeApiName")},
            {
                "start_provision_amount": a.const("32001"),
                "end_provision_amount": a.const("64000"),
                # Start and end usage added automatically, so turn those off.
                "start_usage_amount": a.const(None),
                "end_usage_amount": a.const(None),
            },
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("iops"),
        )


class EBSIOPSIO2Tier3Handler(AWSBaseHandler):
    TF = "aws_ebs_volume"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.product_usagetype(row, c="IOPS")
            # io2 has special pricing tiers that we need to create in other handlers
            and matchers.product_attr(row, "volumeApiName", v="io2")
            and matchers.product_usagetype(row, c="tier3")
        )

    def process(self, row):
        return process(
            row,
            {"values.type": a.product("volumeApiName")},
            {
                "start_provision_amount": a.const("64001"),
                "end_provision_amount": a.const("Inf"),
                # Start and end usage added automatically, so turn those off.
                "start_usage_amount": a.const(None),
                "end_usage_amount": a.const(None),
            },
            a.priced_by_ops,
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("iops"),
        )


class DynamoDBStorageHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and matchers.product_usagetype(row, c="TimedStorage")
            and not matchers.product_usagetype(row, c="IA-TimedStorage")
        )

    def process(self, row):
        return process(
            row,
            {},
            {
                "table_class": a.const("standard"),
            },
            a.priced_by({"gb-mo": "d"}),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("storage"),
        )


class DynamoDBStorageIAHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and matchers.product_usagetype(row, c="IA-TimedStorage")
        )

    def process(self, row):
        return process(
            row,
            {
                "values.table_class": a.const("STANDARD_INFREQUENT_ACCESS"),
            },
            {
                "table_class": a.const("ia"),
            },
            a.priced_by({"gb-mo": "d"}),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("storage"),
        )


class DynamoDBRequestsHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and (
                (
                    matchers.product_usagetype(row, c="ReadRequestUnits")
                    and not matchers.product_usagetype(row, c="IA-ReadRequestUnits")
                )
                or (
                    matchers.product_usagetype(row, c="WriteRequestUnits")
                    and not matchers.product_usagetype(row, c="IA-WriteRequestUnits")
                )
            )
        )

    def process(self, row):
        return process(
            row,
            {},
            {
                "request_type": a.product("usagetype", t=self.request_type),
                "table_class": a.const("standard"),
            },
            a.priced_by(
                {
                    "WriteRequestUnits": "o",
                    "ReadRequestUnits": "o",
                }
            ),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("requests"),
        )

    def request_type(self, attr):
        if "Read" in attr:
            return "read"
        elif "Write" in attr:
            return "write"
        else:
            raise Exception("Unknown request_type: {}".format(attr))


class DynamoDBRequestsIAHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and (
                matchers.product_usagetype(row, c="IA-ReadRequestUnits")
                or matchers.product_usagetype(row, c="IA-WriteRequestUnits")
            )
        )

    def process(self, row):
        return process(
            row,
            {
                "values.table_class": a.const("STANDARD_INFREQUENT_ACCESS"),
            },
            {
                "request_type": a.product("usagetype", t=self.request_type),
                "table_class": a.const("ia"),
            },
            a.priced_by(
                {
                    "WriteRequestUnits": "o",
                    "ReadRequestUnits": "o",
                }
            ),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("storage"),
        )

    def request_type(self, attr):
        if "Read" in attr:
            return "read"
        elif "Write" in attr:
            return "write"
        else:
            raise Exception("Unknown request_type: {}".format(attr))


class DynamoDBReplHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and matchers.product_usagetype(row, c="ReplWriteCapacity")
            and not matchers.product_usagetype(row, c="IA-ReplWriteCapacity")
        )

    def process(self, row):
        return process(
            row,
            {
                "values.replica.region_name": a.product("regionCode"),
            },
            {
                "table_class": a.const("standard"),
                "region": a.const(None),
            },
            a.priced_by({"ReplicatedWriteCapacityUnit-Hrs": "o"}),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("replication"),
        )


class DynamoDBReplIAHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and matchers.product_usagetype(row, c="IA-ReplWriteCapacity")
        )

    def process(self, row):
        return process(
            row,
            {
                "values.table_class": a.const("STANDARD_INFREQUENT_ACCESS"),
                "values.replica.region_name": a.product("regionCode"),
            },
            {
                "table_class": a.const("ia"),
                "region": a.const(None),
            },
            a.priced_by({"ReplicatedWriteCapacityUnit-Hrs": "o"}),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("replication"),
        )


class DynamoDBStreamsHandler(AWSBaseHandler):
    TF = "aws_dynamodb_table"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonDynamoDB")
            and matchers.product_usagetype(row, c="Streams-Requests")
        )

    def process(self, row):
        return process(
            row,
            {},
            {
                "request_type": a.const("stream"),
            },
            a.priced_by({"requests": "o"}),
            service_provider="aws",
            tf_resource=self.TF,
            service_class=a.const("requests"),
        )
