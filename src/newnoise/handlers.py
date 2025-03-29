from . import data, matchers, transforms


PER_TIME = set([
    v.lower()
    for v in [
            "Hrs",
            "Hours",
            "vCPU-hour",
            "vCPU-Months",
            "vCPU-Hours",
            "ACU-Hr",
            "ACU-hour",
            "ACU-Months",
            "Bucket-Mo",
    ]
])

PER_OPERATION = set([
    v.lower()
    for v in [
            "Op",
            "IOPS-Mo",
            "Requests",
            "API Requests",
            "IOs",
            "Jobs",
            "Updates",
            "CR-Hr",
            "API Calls",
    ]
])

PER_DATA = set([
    v.lower()
    for v in [
            "GB-Mo",
            "MBPS-Mo",
            "GB",
            "Objects",
            "Gigabyte Month",
            "Tag-Mo",
            "GB-month",
    ]
])

IGNORE_UNITS = set([
    v.lower()
    for v in [
            'Quantity'
    ]
])


def assert_usage_amount_all_or_nothing(iter):
    for product_match_set, pricing_match_set, price_data in iter:
        def has_usage(pi):
            return 'start_usage_amount' in pi or 'end_usage_amount' in pi

        if any([has_usage(pi) for pi in pricing_match_set]) and \
           not all([has_usage(pi) for pi in pricing_match_set]):
            raise Exception(
                'Missing startUsageAmount or endUsageAmount {} {} {}'.format(
                    product_match_set,
                    pricing_match_set,
                    price_data))

        yield (product_match_set, pricing_match_set, price_data)


def attr(key, attr_data, t=None):
    if key in attr_data:
        attr = attr_data[key]
        if t:
            attr = t(attr)
        return attr
    else:
        return None


def product(key, t=None):
    def f(row, _):
        return attr(key, row[data.ATTRIBUTES], t=t)
    return f


def price(key, t=None):
    def f(_, price_attrs):
        return attr(key, price_attrs, t=t)
    return f


def priced_by_time(_row, price_attrs):
    unit = price_attrs['unit'].lower()
    if unit in PER_TIME:
        return 't'
    elif unit in IGNORE_UNITS or unit in PER_OPERATION or unit in PER_DATA:
        return None
    else:
        raise Exception('price_by_hours unknoown unit: {}'.format(price_attrs))


def priced_by_ops(_row, price_attrs):
    unit = price_attrs['unit'].lower()
    if unit in PER_OPERATION:
        return 'o'
    elif unit in IGNORE_UNITS or unit in PER_TIME or unit in PER_DATA:
        return None
    else:
        raise Exception('price_by_operations unknoown unit: {}'.format(price_attrs))


def priced_by_data(_row, price_attrs):
    unit = price_attrs['unit'].lower()
    if unit in PER_DATA:

        return 'd'
    elif unit in IGNORE_UNITS or unit in PER_TIME or unit in PER_OPERATION:
        return None
    else:
        raise Exception('price_by_data unknoown unit: {}'.format(price_attrs))


def const(v):
    def f(_row, _price_attrs):
        return v
    return f


def with_(d, v):
    d.update(v)
    return d


def normalize_purchase_option(attr):
    if attr == 'OnDemand':
        return 'on_demand'
    else:
        return attr


def normalize_provision(attr):
    amount, unit = attr.split()
    amount = int(amount)
    match unit:
        case 'TB':
            return str(amount * 1000)
        case 'GB':
            return str(amount)


def process(row, product_ms, pricing_ms, priced_by, service_provider, tf_resource, service_class):
    return data.process(
            row,
            with_({'type': const(tf_resource)}, product_ms),
            with_(
                {
                    'end_provision_amount': product('maxVolumeSize', t=normalize_provision),
                    'end_usage_amount': price('endUsageAmount'),
                    'purchase_option': price('purchaseOption', t=normalize_purchase_option),
                    'region': product('regionCode'),
                    'service_class': service_class,
                    'service_provider': const(service_provider),
                    'start_provision_amount': product('minVolumeSize', t=normalize_provision),
                    'start_usage_amount': price('startUsageAmount'),
                },
                pricing_ms),
            priced_by)


class BaseHandler:
    SERVICE_PROVIDER = 'overwrite with service provider'
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
    SERVICE_PROVIDER = 'aws'


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
                "values.instance_type": product('instanceType'),
            },
            {
                'purchase_option': price('purchaseOption'),
                'os': product('operatingSystem', t=str.lower)
            },
            priced_by_time,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('instance'))


class EC2InstanceHandler(BaseInstanceHandler):
    TF = "aws_instance"

    def match(self, row):
        # Only match Linux and Windows instance
        return (
            super().match(row)
            and (matchers.product_operation(row, v="RunInstances")
                 # Windows
                 or matchers.product_operation(row, v="RunInstances:0002"))
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
        return process(
            row,
            {
                self.KEY_LBT: product('operation', t=self.t_operation),
            },
            {},
            priced_by_data,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('data'))

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
        'Any': None,
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
        engine = self.ENGINE_LOOKUP[product('databaseEngine')(row, price_attrs)]
        edition = self.EDITION_LOOKUP[product('databaseEdition')(row, price_attrs)]
        if edition:
            engine = '-'.join([engine, edition])
        return engine

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
            and not matchers.product_attr(row, 'databaseEdition', c='BYOM')
            and (
                matchers.product_usagetype(row, v='InstanceUsage')
                or matchers.product_usagetype(row, c='InstanceUsage:')
            )
        )

    def process(self, row):
        return process(
            row,
            {
                'values.engine': self.a_engine,
                'values.instance_class': product('instanceType'),
                'values.multi_az': product('deploymentOption', t=self.t_deployment_option),
                # 'values.license_model': product('licenseModel', t=self.t_license_model)
            },
            {},
            priced_by_time,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('instance'))


class RDSIOPSHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and not matchers.product_attr(row, 'databaseEdition', c='BYOM')
            and (
                (matchers.product_usagetype(row, s='StorageIOUsage')
                 and matchers.product_attr(row, 'volumeType', v='Magnetic'))
                or matchers.product_usagetype(row, s='GP3-PIOPS')
                or matchers.product_usagetype(row, s='Multi-AZ-GP3-PIOPS')
                or matchers.product_usagetype(row, s='RDS:PIOPS')
                or matchers.product_usagetype(row, s='RDS:Multi-AZ-PIOPS')
                or matchers.product_usagetype(row, s='RDS:IO2-PIOPS')
                or matchers.product_usagetype(row, s='RDS:Multi-AZ-IO2-PIOPS')
            )
        )

    def process(self, row):
        return process(
            row,
            {
                'values.engine': self.a_engine,
                'values.storage_type': product('usagetype', t=self.storage_type)
            },
            {},
            priced_by_ops,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('iops'))

    def storage_type(self, usagetype):
        if usagetype.endswith('StorageIOUsage'):
            return 'standard'
        elif usagetype.endswith('GP3-PIOPS') or usagetype.endswith('Multi-AZ-GP3-PIOPS'):
            return 'gp3'
        elif usagetype.endswith('RDS:PIOPS') or usagetype.endswith('RDS:Multi-AZ-PIOPS'):
            return 'io1'
        elif usagetype.endswith('RDS:IO2-PIOPS') or usagetype.endswith('RDS:Multi-AZ-IO2-PIOPS'):
            return 'io2'
        else:
            raise Exception('Invalid storage_type: {}'.format(attr))



class RDSStorageHandler(RDSBaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return (
            matchers.product_servicecode(row, v="AmazonRDS")
            and not matchers.product_attr(row, 'databaseEdition', c='BYOM')
            and not matchers.product_attr(row, 'databaseEngine', v='Any')
            and (
                matchers.product_usagetype(row, s='StorageUsage')
                or matchers.product_usagetype(row, s='GP2-Storage')
                or matchers.product_usagetype(row, s='GP3-Storage')
                or matchers.product_usagetype(row, s='PIOPS-Storage')
                or matchers.product_usagetype(row, s='PIOPS-Storage-IO2')
            )
            and not matchers.product_usagetype(row, c='Mirror')
            and not matchers.product_usagetype(row, c='Cluster')
        )

    def process(self, row):
        return process(
            row,
            {
                'values.engine': self.a_engine,
                'values.multi_az': product('deploymentOption', t=self.t_deployment_option),
                # 'values.license_model': product('licenseModel', t=self.t_license_model)
                'values.storage_type': product('usagetype', t=self.storage_type),
            },
            {
                # Start and end usage added automatically, so turn those off.
                'start_usage_amount': const(None),
                'end_usage_amount': const(None),
            },
            priced_by_data,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('storage'))

    def storage_type(self, usagetype):
        if usagetype.endswith('StorageUsage'):
            return 'standard'
        elif usagetype.endswith('GP2-Storage'):
            return 'gp2'
        elif usagetype.endswith('GP3-Storage'):
            return 'gp3'
        elif usagetype.endswith('PIOPS-Storage'):
            return 'io1'
        elif usagetype.endswith('PIOPS-Storage-IO2'):
            return 'io2'
        else:
            raise Exception('Invalid storage_type: {}'.format(attr))


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
        return process(
            row,
            {},
            {
                "tier": product('group', t=self.t_tier),
            },
            priced_by_ops,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('requests'))

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
            and matchers.product_usagetype(row, c='-TimedStorage-')
        )

    def process(self, row):
        return process(
            row,
            {},
            {
                'storage_class': product('storageClass', t=self.storage_class)
            },
            priced_by_data,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('storage'))

    def storage_class(self, attr):
        return attr.lower().replace(' ', '_').replace('-', '_')


class SQSFIFOHandler(AWSBaseHandler):
    TF = "aws_sqs_queue"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AWSQueueService")
            and matchers.product_usagetype(row, c='Requests')
            and matchers.product_usagetype(row, c='Requests-FIFO')
        )

    def process(self, row):
        return process(
            row,
            {
                'values.fifo_queue': const('false'),
            },
            {},
            priced_by_ops,
            service_provider='aws',
            tf_resource=self.TF,
            service_class=const('requests'))


class SQSHandler(AWSBaseHandler):
    TF = "aws_sqs_queue"

    def match(self, row):
        return (
            super().match(row)
            and matchers.product_servicecode(row, v="AWSQueueService")
            and matchers.product_usagetype(row, c='Requests')
            and not matchers.product_usagetype(row, c='Requests-FIFO')
        )


    def process(self, row):
        return process(
            row,
            {
                'values.fifo_queue': const('true'),
            },
            {},
            priced_by_ops,
            service_provider='aws',
            tf_resource=self.TF,
            service_class='requests')
