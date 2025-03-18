from . import data, matchers


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

    def match(self, row):
        return True

    def reduce(self, row):
        return row

    def transform(self, match_set, price_info):
        return match_set, price_info

    def price(self, row, currency):
        return data.reduce(row, product_attrs=[], price_attrs={currency: currency})


class BaseInstanceHandler(BaseHandler):
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
            # keep all price info
            price_attrs=None,
        )

    def transform(self, match_set, price_info):
        match_set = clean_usage_type(match_set)
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
            price_attrs=None,
            #price_attrs={
            #    "purchaseOption": "purchase_option",
            #    "USD": "USD",
            #},
        )

    def transform(self, match_set, price_info):
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
                "databaseEngine": "database_engine",
                "engineCode": "engine_code",
                "instanceType": "instance_type",
                "deploymentOption": "deployment_option",
                "databaseEdition": "database_edition",
                "licenseModel": "license_model",
            },
            price_attrs=None,
            #price_attrs={
            #    "purchaseOption": "purchase_option",
            #    "USD": "USD",
            #},
        )

    def transform(self, match_set, price_info):
        match_set = clean_usage_type(match_set)
        return match_set, price_info


class S3Handler(BaseHandler):
    TF = "aws_s3_bucket"

    def match(self, row):
        return matchers.product_servicecode(row, v="AmazonS3")

    def reduce(self, row):
        return data.reduce(
            row,
            product_attrs={
                "operation": "operation",
                "usagetype": "usage_type",
                "feeCode": "fee_code",
                "feeDescription": "fee_description",
                "group": "group",
                "groupDescription": "groupDescription",
            },
            price_attrs=None,
            #price_attrs={
            #    "purchaseOption": "purchase_option",
            #    "USD": "USD",
            #},
        )


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
            price_attrs=None,
            #price_attrs={
            #    "purchaseOption": "purchase_option",
            #    "USD": "USD",
            #},
        )
