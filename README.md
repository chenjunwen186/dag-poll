# PoC of using MerkleDAG to sync DAG state incrementally

## How to use

```bash
# Init DAG
# This will create a DAG on ./.dag/from.json
./actions create

# Start the observable server
# This server will listen to the ./.dag/from.json changed event,
# and auto reload the ./.dag/from.json
./up observable

# Start the observer server
# This will sync the DAG state from observable incrementally
# Synced DAG state will be saved on ./.dag/to.json
./up observer

# To random modify the ./.dag/from.json
./actions random

# To compare the ./.dag/from.json and ./.dag/to.json
./actions isequal
```
