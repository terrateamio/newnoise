from . import data, matchers, transforms


def attr(key, attr_data, t=None):
    if key in attr_data:
        attr =  attr_data[key]
        if callable(t):
            attr = t(attr)
        return attr
    else:
        return None


def attr_product(key, t=None):
    def f(attr_data):
        return attr(key, attr_data, t=t)
    return f


def attr_price(key, t=None):
    def f(price_data, _):
        return attr(key, price_data, t=t)
    return f


def attr_from_product(key, t=None):
    def f(_, product_data):
        return attr(key, product_data, t=t)
    return f


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

    def process(self, row):
        clean_ut = transforms.mk_clean_fun(p='-', s=':')

        product_data = data.process_product_skel(row, {
            "values.instance_type": attr_product('instanceType'),
            "usage_type": attr_product('usagetype', t=clean_ut)
        })

        (price_datas, oiq_prices) = data.process_price_skel(row, {
            'service_class': 'instance',
        })

        return product_data, price_datas, oiq_prices


class InstanceHandler(BaseInstanceHandler):
    TF = "aws_instance"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_operation(row, v="RunInstances")
            and matchers.product_usagetype(
                row, p="UnusedBox", t=transforms.mk_clean_fun(p='-', s=':')
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
                row, p="UnusedDed", t=transforms.mk_clean_fun(p='-', s=':')
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
        product_match_set = data.process_product_skel(row, {
            self.KEY_LBT: attr_product('operation', t=self.t_operation),
        })

        (price_match_sets, oiq_prices) = data.process_price_skel(row, {})

        return product_match_set, price_match_sets, oiq_prices

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

    EDITION_LOOKUP = {
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

    def a_engine(self):
        def f(attr_data):
            engine = self.ENGINE_LOOKUP[attr_data.get('databaseEngine')]
            edition = self.EDITION_LOOKUP[attr_data.get('databaseEdition', None)]
            if edition:
                engine = '-'.join([engine, edition])
            return engine
        return f

    def t_license_model(self,  lm):
        match lm:
            case "Bring your own license":
                return "bring-your-own-license"
            case "License included":
                return "license-included"
            case _:
                return lm

    def t_deployment_option(self,  do):
        match do:
            case 'Single-AZ':
                return "false"
            case 'Multi-AZ':
                return "true"


class RDSInstanceHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and (
                matchers.product_usagetype(row, v='InstanceUsage')
                or matchers.product_usagetype(row, s='InstanceUsage:')
            )
        )

    def process(self, row):
        product_match_set = data.process_product_skel(row, {
            'values.engine': self.a_engine(),
            'values.instance_class': attr_product('instanceType'),
            'values.multi_az': attr_product('deploymentOption', t=self.t_deployment_option),
            'values.license_model': attr_product('licenseModel', t=self.t_license_model)
        })

        (price_match_sets, oiq_prices) = data.process_price_skel(row, {
            'service_class': 'instance',
        })

        return product_match_set, price_match_sets, oiq_prices


class RDSIOPSHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and matchers.product_group(row, v='RDS I/O Operation')
        )

    def process(self, row):
        product_match_set = data.process_product_skel(row, {})

        (price_match_sets, oiq_prices) = data.process_price_skel(row, {
            'service_class': 'instance',
        })

        return product_match_set, price_match_sets, oiq_prices


class RDSStorageHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and matchers.product_usagetype(row, s='StorageUsage')
        )

    def process(self, row):
        product_match_set = data.process_product_skel(row, {
            'values.engine': self.a_engine(),
            'values.instance_class': attr_product('instanceType'),
            'values.multi_az': attr_product('deploymentOption', t=self.t_deployment_option),
            'values.license_model': attr_product('licenseModel', t=self.t_license_model)
        })

        (price_match_sets, oiq_prices) = data.process_price_skel(row, {
            'service_class': 'instance',
        })

        return product_match_set, price_match_sets, oiq_prices


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

    def process(self, row):
        product_data = data.process_product_skel(row, {})

        if len(row) < data.PRICES:
            return product_data, None, None

        (price_datas, oiq_prices) = data.process_price_skel(row, {
            "tier": attr_from_product('group', t=self.t_tier),
        })

        return product_data, price_datas, oiq_prices

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
        return (
            matchers.product_servicecode(row, v="AmazonS3")
            and matchers.product_usagetype(row, s='-TimedStorage-')
        )

    def process(self, row):
        product_data = data.process_product_skel(row, {})
        (price_datas, oiq_prices) = data.process_price_skel(row, {
            'service_class': 'storage',
        })
        return product_data, price_datas, oiq_prices


class SQSHandler(AWSBaseHandler):
    TF = "aws_sqs_queue"

    def match(self, row):
        return matchers.product_servicecode(row, v="AWSQueueService")

    def process(self, row):
        product_match_set = data.process_product_skel(row, {
            'values.usage_type': attr_product('usagetype'),
            'values.queue_type': attr_product('queueType'),
            'values.deliver_order': attr_product('messageDeliveryOrder'),
        })

        (price_match_sets, oiq_prices) = data.process_price_skel(row, {
            'service_class': 'instance',
        })

        return product_match_set, price_match_sets, oiq_prices

