# concurrency-control-protocols
Simulation of protocols used in Database Management Systems in order to control concurrency

Currently, there is only one protocol being simulated which is based on Timestamps.
The plan is to keep implementing more protocols frequently.

In order to run the simulation for the Timestamps based protocol you can execute the following command in a terminal from within this repository:

```python timestamp.py log.txt [-twr] ```

The `-twr` means you are going to simulate the protocol taking into consideration the [Thomas Write Rule](https://en.wikipedia.org/wiki/Thomas_write_rule). Note that this argument is optional so you could just remove it if you want to run in the standard mode, which is without applying this rule.

The `log.txt` file should list all the operations that you want to simulate in the following order and in separate lines:

```[transaction_name] [operation] [data_item]```

In the future is intended to use some kind of further representation of the transaction log, such as a table or timeline listing all the transactions being simulated. Something like the [`tabulate`](https://pypi.python.org/pypi/tabulate) library would be a good option.

Thanks for reading this and I would love to receive some Pull Requests!
