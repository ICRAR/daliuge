[
	{
		"oid": "SL_A",
		"type": "socket",
		"port": 1111,
		"reuse_addr": true,
		"outputs": ["A"]
	},
	{
		"oid": "SL_B",
		"type": "socket",
		"port": 1112,
		"reuse_addr": true,
		"outputs": ["B"]
	},
	{
		"oid": "SL_C",
		"type": "socket",
		"port": 1113,
		"reuse_addr": true,
		"outputs": ["C"]
	},
	{
		"oid": "SL_D",
		"type": "socket",
		"port": 1114,
		"reuse_addr": true,
		"outputs": ["D"]
	},
	{
		"oid": "A",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["E"]
	},
	{
		"oid": "B",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["I"]
	},
	{
		"oid": "C",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["F"]
	},
	{
		"oid": "D",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["F"]
	},
	{
		"oid": "E",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["G"]
	},
	{
		"oid": "F",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["H"]
	},
	{
		"oid": "G",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["I"]
	},
	{
		"oid": "H",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["I"]
	},
	{
		"oid": "I",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["J"]
	},
	{
		"oid": "J",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["L", "M"]
	},
	{
		"oid": "SL_K",
		"type": "socket",
		"port": 1115,
		"reuse_addr": true,
		"outputs": ["K"]
	},
	{
		"oid": "K",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["M"]
	},
	{
		"oid": "L",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["N", "O"]
	},
	{
		"oid": "M",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["P"]
	},
	{
		"oid": "N",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["Q"]
	},
	{
		"oid": "O",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["R"]
	},
	{
		"oid": "P",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["R"]
	},
	{
		"oid": "Q",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["S"]
	},
	{
		"oid": "R",
		"type": "app",
		"app": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["T"]
	},
	{
		"oid": "S",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP"
	},
	{
		"oid": "T",
		"type": "data",
		"dataclass": "dlg.data.drops.memory.InMemoryDROP"
	}
]
