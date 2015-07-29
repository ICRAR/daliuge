[
	{
		"oid":       "A",
		"type":      "socket",
		"storage":   "memory",
		"port":      1111,
		"consumers": ["E"]
	},
	{
		"oid":       "B",
		"type":      "socket",
		"storage":   "memory",
		"port":      1112
	},
	{
		"oid":       "C",
		"type":      "socket",
		"storage":   "memory",
		"port":      1113
	},
	{
		"oid":       "D",
		"type":      "socket",
		"storage":   "memory",
		"port":      1114
	},
	{
		"oid":       "E",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory"
	},
	{
		"oid":       "F",
		"type":      "container",
		"children":  ["C", "D"]
	},
	{
		"oid":       "G",
		"type":      "container",
		"children":  ["E", "F"],
		"consumers": ["H"]
	},
	{
		"oid":       "H",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory",
		"consumers": ["J"]
	},
	{
		"oid":       "I",
		"type":      "socket",
		"storage":   "memory",
		"port":      1115
	},
	{
		"oid":       "J",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory",
		"consumers": ["M", "N"]
	},
	{
		"oid":       "K",
		"type":      "container",
		"children":  ["H", "I"],
		"consumers": ["L"]
	},
	{
		"oid":       "L",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory"
	},
	{
		"oid":       "M",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory",
		"consumers": ["O"]
	},
	{
		"oid":       "N",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory",
		"consumers": ["P"]
	},
	{
		"oid":       "O",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory"
	},
	{
		"oid":       "P",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"storage":   "memory"
	},
	{
		"oid":       "Q",
		"type":      "container",
		"children":  ["L", "P"]
	}
]