from . import cleaners, data, matchers


def clean_usage_type(match_set):
    if "usage_type" in match_set:
        usage = match_set["usage_type"].split(":")
        if len(usage) == 2:
            match_set["usage_type"] = usage[0]
    return match_set


class BaseHandler:
    TF = "overwrite with name of TF resource"

    def __init__(self, **match_params):
        self.match_params = match_params

    def clean(self, row):
        pass

    def match(self, row):
        return True

    def match_currency(self, row, ccy=None):
        return matchers.price_currency(row, ccy=ccy)

    def reduce(self, row):
        return data.reduce(row, product_attrs=None, price_attrs=None)

    def transform(self, match_set, price_info):
        return match_set, price_info


class BaseInstanceHandler(BaseHandler):
    def match(self, row):
        if (
            super().match(row)
            and matchers.product_servicecode(row, v="AmazonEC2")
            and matchers.required_attrs(row, ["instanceType"])
        ):
            # clean happens only after confirming it is a type
            # known to have the issue
            self.clean(row) 
            return True
        return False

    def clean(self, row):
        new_ut = cleaners.usage_type(row)
        row[data.ATTRIBUTES]['usagetype'] = new_ut

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


class LoadBalancerHandler(BaseHandler):
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


class RDSHandler(BaseHandler):
    TF = "aws_db_instance"

    def match(self, row):
        return matchers.product_servicecode(row, v="AmazonRDS")

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                # "databaseEngine": "database_engine",
                "engineCode": "engine_code",
                "instanceType": "instance_type",
                "deploymentOption": "deployment_option",
                # "databaseEdition": "database_edition",
                "licenseModel": "license_model",
            },
            price_attrs={},
        )

    def transform(self, match_set, price_info):
        (match_set, price_info) = super().transform(match_set, price_info)
        match_set = clean_usage_type(match_set)

        if 'license_model' in match_set:
            if match_set['license_model'] == "Bring your own license":
                match_set['license_model'] = "bring-your-own-license"
            elif match_set['license_model'] == "License included":
                match_set['license_model'] = "license-included"

        if 'deployment_option' in match_set:
            if match_set['deployment_option'] == 'Single-AZ':
                match_set['multi_az'] = "false"
            elif match_set['deployment_option'] == 'Multi-AZ':
                match_set['multi_az'] = "true"
            del match_set['deployment_option']

        return match_set, price_info


class S3Handler(BaseHandler):
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

    def transform(self, product_info, price_info):
        group = product_info['group']
        del product_info['group']

        # s3 has one price row
        price = price_info[0]
        if group == "S3-API-Tier1":
            price['tier'] = "1"
        elif group == "S3-API-Tier2":
            price['tier'] = "2"

        return product_info, price_info


class SQSHandler(BaseHandler):
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
