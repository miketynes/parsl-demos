"""A bag of (fake) tasks

My goal is to provision a (small) set of nodes on the cluster
and send a set of tasks to them
"""

from concurrent.futures import as_completed

from tqdm.auto import tqdm

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.providers import PBSProProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher


@python_app
def random_sleep_task(
    task_ix: int,
    sleep_min: int, 
    sleep_max: int,            
    ):
    """Sleep for a moment and then say hello
    """
    import time
    import random

    sleep_duration = random.randint(sleep_min, sleep_max)
    time.sleep(sleep_duration)

    # dont print hello, let the main thread do that
    # (we dont want to deal with stdout yet)
    msg = f'Hello from task {task_ix} (slept for {sleep_duration}'
    return msg

if __name__ == "__main__": 


    
    n_tasks = 10
    min_sleep = 15
    max_sleep = 30

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='htex',
                provider=PBSProProvider(
                    account='Athena',
                    queue='debug',
                    nodes_per_block=1,
                    walltime='00:10:00',
                    init_blocks=1, 
                    max_blocks=1,
                    worker_init='/home/mtynes/miniconda3/bin/activate parsl-demos',
                    launcher=SimpleLauncher(),
                ),
            )
        ],
    )

    parsl.load(config)

    futures = []
    for task_ix in range(n_tasks):
        msg = random_sleep_task(
            task_ix=task_ix,
            min_sleep=min_sleep,
            max_sleep=max_sleep,
        )
        futures.append(msg)
    for future in tqdm(as_completed(futures), total=len(futures)):
        if future.exception() is not None:
            print(future.exception())
        
        msg = future.result()
        print(msg)
    