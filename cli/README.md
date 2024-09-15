## Supported commands

- `create`
- `lock`
- `unlock`
- `reconfigure`
- `get`
- `set`
- `invokeMethod`

Use `[command] --help` to get usage information for a specific command.

## Example

Create a distributed counter with the name `myCounter`:
```
create myCounter -t SharedCounter
```

Increment the counter by 5:
```
invokeMethod myCounter increment -a 5
```

Lock the counter:
```
lock myCounter
```

You now have exclusive access to the counter. 

Use the `--lock-token` (or `-l`) flag with the lock token returned by the `lock` command to invoke methods on the counter with the lock held:
```
invokeMethod myCounter increment -a 5 -l 1
```

Commands without a lock token will get queued and executed once the lock is released.

Unlock the counter:
```
unlock myCounter 1
```
