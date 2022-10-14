[
	{
		"oid":       "SL_A",
		"type":      "socket",
		"port":      1111,
		"reuse_addr": true,
		"outputs": ["A"]
	},
	{
		"oid":       "SL_B",
		"type":      "socket",
		"port":      1112,
		"reuse_addr": true,
		"outputs": ["B"]
	},
	{
		"oid":       "SL_C",
		"type":      "socket",
		"port":      1113,
		"reuse_addr": true,
		"outputs": ["C"]
	},
	{
		"oid":       "SL_D",
		"type":      "socket",
		"port":      1114,
		"reuse_addr": true,
		"outputs": ["D"]
	},
	{
		"oid":       "A",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["E"]
	},
	{
		"oid":       "B",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["I"]
	},
	{
		"oid":       "C",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["F"]
	},
	{
		"oid":       "D",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["F"]
	},
	{
		"oid":       "E",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["G"]
	},
	{
		"oid":       "F",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["H"]
	},
	{
		"oid":       "G",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["I"]
	},
	{
		"oid":       "H",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["I"]
	},
	{
		"oid":       "I",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["J"]
	},
	{
		"oid":       "J",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["L", "M"]
	},
	{
		"oid":       "SL_K",
		"type":      "socket",
		"port":      1115,
		"reuse_addr": true,
		"outputs": ["K"]
	},
	{
		"oid":       "K",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["M"]
	},
	{
		"oid":       "L",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["N", "O"]
	},
	{
		"oid":       "M",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["P"]
	},
	{
		"oid":       "N",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["Q"]
	},
	{
		"oid":       "O",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["R"]
	},
	{
		"oid":       "P",
		"type":      "data",
		"storage":   "Memory",
		"consumers": ["R"]
	},
	{
		"oid":       "Q",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["S"]
	},
	{
		"oid":       "R",
		"type":      "app",
		"app":       "dlg.apps.simple.SleepAndCopyApp",
		"outputs":   ["T"]
	},
	{
		"oid":       "S",
		"type":      "data",
		"storage":   "Memory"
	},
	{
		"oid":       "T",
		"type":      "data",
		"storage":   "Memory"
	}
]
