from base import *

# GLOBAL VARIABLES
TWR = False
log = []
data = {}
ts_mapper = {}

# DEBUG METHODS
def debug():
  code.interact(local=dict(globals(), **locals()))

# SETUP METHODS
def check_twr_option(args):
  global TWR

  if len(args) > 2:
    if args[2] == '-twr':
      TWR = True
      print "INFO: Thomas Write Rule applied."
    else:
      print "INFO: Unknown option '{0}'. Running simulation in standard mode.".format(args[2])

def get_timestamp(ts_name):
  global ts_mapper
  try:
    return ts_mapper[ts_name]['timestamp']
  except (KeyError, NameError):
    print "No transaction found!"

def set_timestamp(ts, value):
  ts["timestamp"] = value

def get_data_write_timestamp(data_name):
  return data[data_name]['w_ts']

def get_data_read_timestamp(data_name):
  return data[data_name]['r_ts']

def set_data_write_timestamp(data_name, value):
  data[data_name]['w_ts'] = value

def set_data_read_timestamp(data_name, value):
  data[data_name]['r_ts'] = value

def map_new_transaction(ts_name, value):
  global ts_mapper
  ts_mapper[ts_name] = {'timestamp': value, 'status': 'new'}

def populate_transaction_log(file):
  global log

  for line in fileinput.FileInput(file):
    info = line.split()
    log.append({"transaction": info[0], "action": info[1], "data": info[2], "timestamp": None})
  # debug()

def set_transactions_timestamp(log_array):
  global ts_mapper
  checked_transactions = []
  ts_counter = 1

  for op in log_array:
    ts_name = op['transaction']
    if ts_name in checked_transactions:
      set_timestamp(op, ts_mapper[ts_name]['timestamp'])
    else:
      checked_transactions.append(ts_name)
      map_new_transaction(ts_name, ts_counter)
      set_timestamp(op, ts_counter)
      ts_counter += 1

def set_transaction_status(ts_name, status):
  global ts_mapper
  ts_mapper[ts_name]['status'] = status

def set_transactions_data(log_array):
  global data

  for op in log_array:
    data_name = op['data']
    if not data_name in data:
      data[data_name] = {'w_ts': 0, 'r_ts': 0}

# EXECUTION METHODS
def simulate_read(ts_timestamp, ts_name, data_name,data_read_timestamp, data_write_timestamp):
  global ts_mapper

  if ts_timestamp < data_write_timestamp:
    print "\tRead operation REJECTED!\n\tRunning rollback on transaction {0}...".format(ts_name)
    set_transaction_status(ts_name, 'aborted')
    return 'rejected'
  else:
    print "\tRead operation EXECUTED!\n\tSetting new read timestamp for item {0}...".format(data_name)
    set_data_read_timestamp(data_name, max(data_read_timestamp, ts_timestamp))
    set_transaction_status(ts_name, 'ok')
    print "\tRead timestamp for item '{0}' is now {1}".format(data_name, get_data_read_timestamp(data_name))
    return 'executed'

def simulate_write(ts_timestamp, ts_name, data_name, data_read_timestamp, data_write_timestamp):
  global ts_mapper

  if ts_timestamp < data_read_timestamp or (ts_timestamp < data_write_timestamp and not TWR):
    print "\tWrite operation REJECTED!\n\tRunning rollback on transaction {0}...".format(ts_name)
    set_transaction_status(ts_name, 'aborted')
    return 'rejected'
  elif ts_timestamp < data_write_timestamp and TWR:
    print "\tWrite operation IGNORED!\n"
    set_transaction_status(ts_name, 'ok')
    return 'ignored'
  else:
    print "\tWrite operation EXECUTED!\n\tSetting new write timestamp for item {0}...".format(data_name)
    set_data_write_timestamp(data_name, ts_timestamp)
    set_transaction_status(ts_name, 'ok')
    print "\tWrite timestamp for item '{0}' is now {1}".format(data_name, get_data_write_timestamp(data_name))
    return 'executed'

def run_simulation():
  global log, ts_mapper, data

  print "\n\t>>>> STARTING SIMULATION <<<<"
  for op in log:
    ts_name = op['transaction']
    ts_action = op['action']
    ts_status = ts_mapper[ts_name]['status']
    data_name = op['data']
    ts_timestamp = get_timestamp(ts_name)
    data_read_timestamp = get_data_read_timestamp(data_name)
    data_write_timestamp = get_data_write_timestamp(data_name)

    print "\n> Transaction {0} will execute {1} on item {2}".format(ts_name, ts_action.upper(), data_name)
    if ts_status is not 'aborted':
      if ts_action == 'read':
        op['status'] = simulate_read(ts_timestamp, ts_name, data_name,data_read_timestamp, data_write_timestamp)
      elif ts_action == 'write':
        op['status'] = simulate_write(ts_timestamp, ts_name, data_name, data_read_timestamp, data_write_timestamp)
      else:
        print "Unknown operation '{0}' on transaction {1}!".format(ts_action, ts_name)
    else:
      print "Cannot run transaction {0} because it was aborted!".format(ts_name)

  print "\n\t>>>> SIMULATION FINISHED <<<<\n"

# BASE METHODS
def main():
  global log

  check_twr_option(sys.argv)
  populate_transaction_log(sys.argv[1])
  set_transactions_timestamp(log)
  set_transactions_data(log)
  run_simulation()
  debug()

if __name__ == '__main__':
  main()
