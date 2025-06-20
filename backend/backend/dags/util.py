""" """

import prefect

def make_prefect_task(**kwargs):
    return prefect.task(**kwargs)

def make_prefect_flow(**kwargs):
    return prefect.flow(**kwargs)
