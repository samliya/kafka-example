```bash
go run factor-demo/main.go -operation create -topic my-topic -partitions 3 -replication 3 -min-isr 2
```

```bash
go run factor-demo/main.go -operation check-replica -topic my-topic -check-interval 10
```

```bash
go run factor-demo/main.go -operation update-isr -topic my-topic -min-isr 2
```

