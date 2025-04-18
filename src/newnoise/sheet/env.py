# Set of AWS terms for expressing time based prices
PER_TIME = set(
    [
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
    ]
)

# Set of AWS terms for expressing operation based prices
PER_OPERATION = set(
    [
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
    ]
)

# Set of AWS terms for expressing data transmission based prices
PER_DATA = set(
    [
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
    ]
)

# Unit types that can be ignored
IGNORE_UNITS = set([v.lower() for v in ["Quantity"]])
