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
		"port":      1112,
		"consumers": ["I"]
	},
	{
		"oid":       "C",
		"type":      "socket",
		"storage":   "memory",
		"port":      1113,
		"consumers": ["F"]
	},
	{
		"oid":       "D",
		"type":      "socket",
		"storage":   "memory",
		"port":      1114,
		"consumers": ["F"]
	},
	{
		"oid":       "E",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["G"]
	},
	{
		"oid":       "F",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["H"]
	},
	{
		"oid":       "G",
		"type":      "plain",
		"storage":   "memory",
		"consumers": ["I"]
	},
	{
		"oid":       "H",
		"type":      "plain",
		"storage":   "memory",
		"consumers": ["I"]
	},
	{
		"oid":       "I",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["J"]
	},
	{
		"oid":       "J",
		"type":      "plain",
		"storage":   "memory",
		"consumers": ["L", "M"]
	},
	{
		"oid":       "K",
		"type":      "socket",
		"storage":   "memory",
		"port":      1115,
		"consumers": ["M"]
	},
	{
		"oid":       "L",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["N", "O"]
	},
	{
		"oid":       "M",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["P"]
	},
	{
		"oid":       "N",
		"type":      "plain",
		"storage":   "memory",
		"consumers": ["Q"]
	},
	{
		"oid":       "O",
		"type":      "plain",
		"storage":   "memory",
		"consumers": ["R"]
	},
	{
		"oid":       "P",
		"type":      "plain",
		"storage":   "memory",
		"consumers": ["R"]
	},
	{
		"oid":       "Q",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["S"]
	},
	{
		"oid":       "R",
		"type":      "app",
		"app":       "test.graphsRepository.SleepAndCopyApp",
		"outputs":   ["T"]
	},
	{
		"oid":       "S",
		"type":      "plain",
		"storage":   "memory"
	},
	{
		"oid":       "T",
		"type":      "plain",
		"storage":   "memory"
	}
]