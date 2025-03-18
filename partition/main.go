package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"kafka-examples/conf"

	"github.com/IBM/sarama"
)

var (
	topic          string
	partitionCount int
	replication    int
	operation      string
	minISR         int // 最小同步副本数
	checkInterval  int // 健康检查间隔（秒）
)

func init() {
	// 命令行参数
	flag.StringVar(&topic, "topic", "partition-test-topic", "Topic名称")
	flag.IntVar(&partitionCount, "partitions", 3, "分区数量")
	// 副本因子说明：
	// - 1: 无副本，只有主副本
	// - 2: 一个主副本+一个从副本
	// - 3: 一个主副本+两个从副本（推荐，提供更好的可用性）
	flag.IntVar(&replication, "replication", 3, "副本因子: 设置每个分区的副本数量，用于提供高可用性")
	flag.IntVar(&minISR, "min-isr", 2, "最小同步副本数: 必须保持同步的最小副本数")
	flag.IntVar(&checkInterval, "check-interval", 10, "健康检查间隔（秒）")
	flag.StringVar(&operation, "operation", "create", "操作类型: create/list/increase/delete/check-replica/update-isr")
}

func main() {
	flag.Parse()

	// 创建管理客户端
	admin := createAdmin()
	defer admin.Close()

	switch operation {
	case "create":
		createTopic(admin)
	case "list":
		listTopicDetails(admin)
	case "increase":
		increasePartitions(admin)
	case "delete":
		deleteTopic(admin)
	case "check-replica":
		checkReplicaHealth(admin)
	case "update-isr":
		updateMinISR(admin)
	default:
		fmt.Printf("未知操作: %s\n", operation)
		os.Exit(1)
	}
}

func createAdmin() sarama.ClusterAdmin {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0 // 使用较新的版本

	admin, err := sarama.NewClusterAdmin([]string{conf.HOST}, config)
	if err != nil {
		log.Fatalf("创建管理客户端失败: %v", err)
	}
	return admin
}

// 检查副本健康状态
func checkReplicaHealth(admin sarama.ClusterAdmin) {
	for {
		topics, err := admin.ListTopics()
		if err != nil {
			log.Printf("获取Topic列表失败: %v", err)
			continue
		}

		if detail, exists := topics[topic]; exists {
			partitions, err := admin.DescribeTopics([]string{topic})
			if err != nil {
				log.Printf("获取分区信息失败: %v", err)
				continue
			}

			if len(partitions) > 0 {
				fmt.Printf("\n=== 副本健康状态检查 ===\n")
				fmt.Printf("Topic: %s (副本因子: %d, 最小同步副本: %d)\n",
					topic, detail.ReplicationFactor, minISR)

				for _, partition := range partitions[0].Partitions {
					fmt.Printf("\n分区 %d 状态:\n", partition.ID)

					// 检查Leader是否正常
					if partition.Leader == -1 {
						fmt.Printf("  ⚠️ 警告: 该分区没有Leader!\n")
					} else {
						fmt.Printf("  ✅ Leader正常 (Broker %d)\n", partition.Leader)
					}

					// 检查副本同步状态
					fmt.Printf("  副本状态:\n")
					for _, replica := range partition.Replicas {
						isSync := false
						for _, isr := range partition.Isr {
							if replica == isr {
								isSync = true
								break
							}
						}
						if isSync {
							fmt.Printf("    ✅ Broker %d: 同步中\n", replica)
						} else {
							fmt.Printf("    ❌ Broker %d: 未同步\n", replica)
						}
					}

					// 检查最小同步副本要求
					if len(partition.Isr) < minISR {
						fmt.Printf("  ⚠️ 警告: 同步副本数(%d)小于最小要求(%d)\n",
							len(partition.Isr), minISR)
					} else {
						fmt.Printf("  ✅ 同步副本数满足要求\n")
					}
				}
			}
		} else {
			fmt.Printf("Topic '%s' 不存在\n", topic)
			return
		}

		if checkInterval <= 0 {
			break
		}
		fmt.Printf("\n等待 %d 秒后进行下一次检查...\n", checkInterval)
		time.Sleep(time.Duration(checkInterval) * time.Second)
	}
}

// 更新最小同步副本数
func updateMinISR(admin sarama.ClusterAdmin) {
	// 准备新配置
	minISRStr := fmt.Sprintf("%d", minISR)
	configEntries := map[string]*string{
		"min.insync.replicas": &minISRStr,
	}

	// 更新配置
	err := admin.AlterConfig(sarama.TopicResource, topic, configEntries, false)
	if err != nil {
		log.Fatalf("更新最小同步副本数失败: %v", err)
	}

	fmt.Printf("成功更新Topic '%s' 的最小同步副本数为 %d\n", topic, minISR)

	// 显示更新后的详情
	listTopicDetails(admin)
}

func createTopic(admin sarama.ClusterAdmin) {
	if replication < 1 {
		log.Fatal("副本因子必须大于0")
	}
	if minISR > replication {
		log.Fatal("最小同步副本数不能大于副本因子")
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitionCount),
		ReplicationFactor: int16(replication),
		ConfigEntries: map[string]*string{
			"cleanup.policy":      strPtr("delete"),
			"retention.ms":        strPtr("604800000"), // 7天
			"min.insync.replicas": strPtr(fmt.Sprintf("%d", minISR)),
		},
	}

	err := admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		log.Fatalf("创建Topic失败: %v", err)
	}
	fmt.Printf("成功创建Topic '%s'\n", topic)
	fmt.Printf("配置信息:\n")
	fmt.Printf("- 分区数: %d (用于并行处理)\n", partitionCount)
	fmt.Printf("- 副本因子: %d (用于提供高可用性)\n", replication)
	fmt.Printf("- 最小同步副本: %d (确保数据可靠性)\n", minISR)

	// 等待topic创建完成
	time.Sleep(2 * time.Second)
	listTopicDetails(admin)
}

func listTopicDetails(admin sarama.ClusterAdmin) {
	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("获取Topic列表失败: %v", err)
	}

	if detail, exists := topics[topic]; exists {
		fmt.Printf("\nTopic详情:\n")
		fmt.Printf("名称: %s\n", topic)
		fmt.Printf("分区数: %d\n", detail.NumPartitions)
		fmt.Printf("副本因子: %d\n", detail.ReplicationFactor)

		// 获取分区详细信息
		partitions, err := admin.DescribeTopics([]string{topic})
		if err != nil {
			log.Printf("获取分区信息失败: %v", err)
			return
		}

		if len(partitions) > 0 {
			fmt.Printf("\n分区和副本分布:\n")
			for _, partition := range partitions[0].Partitions {
				fmt.Printf("分区 %d:\n", partition.ID)
				fmt.Printf("  - Leader broker: %d (主副本所在的broker)\n", partition.Leader)
				fmt.Printf("  - 副本分布: %v (所有副本所在的broker列表)\n", partition.Replicas)
				fmt.Printf("  - 同步副本: %v (当前保持同步的副本列表)\n", partition.Isr)
			}
		}
	} else {
		fmt.Printf("Topic '%s' 不存在\n", topic)
	}
}

func increasePartitions(admin sarama.ClusterAdmin) {
	if partitionCount <= 0 {
		log.Fatal("分区数必须大于0")
	}

	err := admin.CreatePartitions(topic, int32(partitionCount), nil, false)
	if err != nil {
		log.Fatalf("增加分区失败: %v", err)
	}
	fmt.Printf("成功将Topic '%s' 的分区数增加到 %d\n", topic, partitionCount)

	// 等待分区创建完成
	time.Sleep(2 * time.Second)
	listTopicDetails(admin)
}

func deleteTopic(admin sarama.ClusterAdmin) {
	err := admin.DeleteTopic(topic)
	if err != nil {
		log.Fatalf("删除Topic失败: %v", err)
	}
	fmt.Printf("成功删除Topic '%s'\n", topic)
}

func strPtr(s string) *string {
	return &s
}
