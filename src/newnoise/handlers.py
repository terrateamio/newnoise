from . import data, matchers


def clean_usage_type(match_set):
    if "usage_type" in match_set:
        usage = match_set["usage_type"].split(":")
        if len(usage) == 2:
            match_set["usage_type"] = usage[0]
    return match_set


# From https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_CreateDBInstance.html
RDS_ENGINE_LOOKUP = {
    'Any': '',
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

RDS_EDITION_LOOKUP = {
    None: "",
    "Advanced": "ae",
    "Enterprise": "ee",
    "Standard": "se",
    "Standard One": "se",
    "Standard Two": "se2",
    "Express": "ex",
    "Web": "web",
    "Developer": "dev",
}


class BaseHandler:
    SERVICE_PROVIDER = 'overwrite with service provider'
    TF = "overwrite with name of TF resource"

    def __init__(self, **match_params):
        self.match_params = match_params

    def match(self, row):
        return True

    def match_currency(self, row, ccy=None):
        return matchers.price_currency(row, ccy=ccy)

    def reduce(self, row):
        return data.reduce(row, product_attrs=None, price_attrs=None)

    def transform(self, match_set, price_info):
        return match_set, price_info


class AWSBaseHandler(BaseHandler):
    SERVICE_PROVIDER = 'aws'


class BaseInstanceHandler(AWSBaseHandler):
    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.required_attrs(row, ["instanceType"])
        )

    def reduce(self, row):
        return data.reduce(
            row,
            # only instance type
            product_attrs={
                "instanceType": "values.instance_type",
            },
            price_attrs={}
        )

    def transform(self, match_set, price_info):
        (match_set, price_info) = super().transform(match_set, price_info)
        match_set = clean_usage_type(match_set)

        for price in price_info:
            price['service_class'] = 'instance'

        return match_set, price_info


class InstanceHandler(BaseInstanceHandler):
    TF = "aws_instance"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_operation(row, v="RunInstances")
            and matchers.product_usagetype(row, p="UnusedBox")
            and matchers.price_purchaseoption(row, v="on_demand")
        )


class EC2HostHandler(BaseInstanceHandler):
    TF = "aws_ec2_host"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_operation(row, v="RunInstances")
            and matchers.product_usagetype(row, p="UnusedDed")
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

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                "operation": self.KEY_LBT,
            },
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        (match_set, price_info) = super().transform(match_set, price_info)
        lbt = match_set[self.KEY_LBT]

        # LoadBalancing :: classic
        if lbt == "LoadBalancing":
            match_set[self.KEY_LBT] = "classic"
        # LoadBalancing:Application
        elif lbt == "LoadBalancing:Application":
            match_set[self.KEY_LBT] = "application"
        # LoadBalancing:Network
        elif lbt == "LoadBalancing:Network":
            match_set[self.KEY_LBT] = "network"
        # LoadBalancing:Gateway
        elif lbt == "LoadBalancing:Gateway":
            match_set[self.KEY_LBT] = "gateway"

        return match_set, price_info


class RDSInstanceHandler(AWSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and (matchers.product_usagetype_raw(row, v='InstanceUsage')
                 or matchers.product_usagetype_raw(row, c='InstanceUsage:'))
            )

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                "databaseEngine": "databaseEngine",
                "databaseEdition": "databaseEdition",
                "instanceType": "values.instance_class",
                # "licenseModel": "licenseModel",
                "deploymentOption": "deploymentOption",
            },
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        (match_set, price_info) = super().transform(match_set, price_info)
        match_set = clean_usage_type(match_set)

        engine = '-'.join(
            [v for v in [
                RDS_ENGINE_LOOKUP[match_set.pop('databaseEngine')],
                RDS_EDITION_LOOKUP[match_set.pop('databaseEdition', None)]
            ]
             if v])

        if engine:
            match_set['values.engine'] = engine

        license_model = match_set.pop('licenseModel', None)

        if license_model:
            if license_model == "Bring your own license":
                match_set['values.license_model'] = "bring-your-own-license"
            elif license_model == "License included":
                match_set['values.license_model'] = "license-included"
            else:
                match_set['values.license_model'] = license_model

        deployment_option = match_set.pop('deploymentOption', None)
        if deployment_option:
            if deployment_option == 'Single-AZ':
                match_set['values.multi_az'] = "false"
            elif deployment_option == 'Multi-AZ':
                match_set['values.multi_az'] = "true"

        for price in price_info:
            price['service_class'] = 'instance'

        return match_set, price_info


class RDSIOPSHandler(AWSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and matchers.product_group(row, v='RDS I/O Operation')
        )

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={},
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        (match_set, price_info) = super().transform(match_set, price_info)
        match_set = clean_usage_type(match_set)

        for price in price_info:
            price['service_class'] = 'iops'

        return match_set, price_info


class RDSStorageHandler(AWSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and matchers.product_usagetype_raw(row, c='StorageUsage')
        )

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                "databaseEngine": "databaseEngine",
                "databaseEdition": "databaseEdition",
                "instanceType": "values.instance_class",
                # "licenseModel": "licenseModel",
                "deploymentOption": "deploymentOption",
            },
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        (match_set, price_info) = super().transform(match_set, price_info)
        match_set = clean_usage_type(match_set)

        engine = '-'.join(
            [v for v in [
                RDS_ENGINE_LOOKUP[match_set.pop('databaseEngine')],
                RDS_EDITION_LOOKUP[match_set.pop('databaseEdition', None)]
            ]
             if v])

        if engine:
            match_set['values.engine'] = engine

        license_model = match_set.pop('licenseModel', None)
        if license_model:
            if license_model == "Bring your own license":
                match_set['values.license_model'] = "bring-your-own-license"
            elif license_model == "License included":
                match_set['values.license_model'] = "license-included"
            else:
                match_set['values.license_model'] = license_model

        deployment_option = match_set.pop('deploymentOption', None)
        if deployment_option:
            if deployment_option == 'Single-AZ':
                match_set['values.multi_az'] = "false"
            elif deployment_option == 'Multi-AZ':
                match_set['values.multi_az'] = "true"

        for price in price_info:
            price['service_class'] = 'storage'

        return match_set, price_info


class S3OperationsHandler(AWSBaseHandler):
    TF = "aws_s3_bucket"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonS3")
            and (
                matchers.product_group(row, v="S3-API-Tier1")
                or matchers.product_group(row, v="S3-API-Tier2")
            )
        )

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                # "operation": "operation",
                # "usagetype": "usage_type",
                "group": "group",
            },
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        group = match_set['group']
        del match_set['group']

        for price in price_info:
            price['service_class'] = 'requests'
            if group == "S3-API-Tier1":
                price['tier'] = "1"
            elif group == "S3-API-Tier2":
                price['tier'] = "2"

        return match_set, price_info


class S3StorageHandler(AWSBaseHandler):
    TF = "aws_s3_bucket"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonS3")
            and matchers.product_usagetype(row, c='-TimedStorage-')
        )

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={},
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        for price in price_info:
            price['service_class'] = 'storage'
        return match_set, price_info


class SQSHandler(AWSBaseHandler):
    TF = "aws_sqs_queue"

    def match(self, row):
        return matchers.product_servicecode(row, v="AWSQueueService")

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                "operation": "operation",
                "usagetype": "usage_type",
                "queueType": "queye_type",
                "deliverFrequency": "delivery_frequency",
                "messageDeliveryOrder": "delivery_order",
            },
            price_attrs={},
        )
