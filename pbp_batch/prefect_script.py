from prefect import flow, task
@task(viz_return_value=[4])
def get_list():
    return [1, 2, 3]

@task
def append_one(n):
    return n.append(6)

@flow
def viz_return_value_tracked():
    l = get_list()
    for num in range(3):
        l.append(5)
        append_one(l)
