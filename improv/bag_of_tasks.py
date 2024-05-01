"""A bag of (fake) tasks

My goal is to provision a (small) set of nodes on the cluster
and send a set of tasks to them
"""
import time
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
    import os

    sleep_duration = random.randint(sleep_min, sleep_max)
    time.sleep(sleep_duration)

    # grab the parsl environment variables so we can print them out
    parsl_env = {k: v for k, v in os.environ.items() if  'parsl' in k.lower()}
    parsl_env = '\n'.join([f'{k}: {v}' for k, v in parsl_env.items()])

    # dont print hello, let the main thread do that
    # (we dont want to deal with stdout yet)
    msg = f"""
Hello from task {task_ix}.
Parsl env:
{parsl_env}
Slept for {sleep_duration}(s).
"""
    return msg, sleep_duration

if __name__ == "__main__": 

    n_tasks = 20
    sleep_min = 1
    sleep_max = 5

    config = Config(
        executors=[
            HighThroughputExecutor(
                label='htex',
                provider=PBSProProvider(
                    account='Athena',
                    queue='debug',
                    nodes_per_block=1,
                    walltime='00:10:00',
                    init_blocks=2,
                    min_blocks=2, 
                    max_blocks=2,
                    worker_init="""
source activate parsl-demos
which python
""",
                    launcher=SimpleLauncher(),
                ),
                max_workers_per_node=1 # number of *parsl* workers
            )
        ],
    )

    parsl.load(config)

    futures = []
    for task_ix in range(n_tasks):
        msg = random_sleep_task(
            task_ix=task_ix,
            sleep_min=sleep_min,
            sleep_max=sleep_max,
        )
        futures.append(msg)
    
    total_sleep = 0
    start = time.perf_counter()
    for future in tqdm(as_completed(futures), total=len(futures)):
        if future.exception() is not None:
            print(future.exception())
        
        msg, sleep_duration = future.result()
        print(msg)
        total_sleep += sleep_duration
    print("Done.")
    elapsed = time.perf_counter() - start
    print("Done.")
    print(f"Total slept time: {total_sleep}s, Wall time: {elapsed:0.2f}s")    