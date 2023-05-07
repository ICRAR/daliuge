[
	{
		"oid": "SL_A",
		"categoryType": "socket",
		"port": 1111,
		"reuse_addr": true,
		"outputs": ["A"]
	},
	{
		"oid": "SL_B",
		"categoryType": "socket",
		"port": 1112,
		"reuse_addr": true,
		"outputs": ["B"]
	},
	{
		"oid": "SL_C",
		"categoryType": "socket",
		"port": 1113,
		"reuse_addr": true,
		"outputs": ["C"]
	},
	{
		"oid": "SL_D",
		"categoryType": "socket",
		"port": 1114,
		"reuse_addr": true,
		"outputs": ["D"]
	},
	{
		"oid": "A",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["E"]
	},
	{
		"oid": "B",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["I"]
	},
	{
		"oid": "C",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["F"]
	},
	{
		"oid": "D",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["F"]
	},
	{
		"oid": "E",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["G"]
	},
	{
		"oid": "F",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["H"]
	},
	{
		"oid": "G",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["I"]
	},
	{
		"oid": "H",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["I"]
	},
	{
		"oid": "I",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["J"]
	},
	{
		"oid": "J",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["L", "M"]
	},
	{
		"oid": "SL_K",
		"categoryType": "socket",
		"port": 1115,
		"reuse_addr": true,
		"outputs": ["K"]
	},
	{
		"oid": "K",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["M"]
	},
	{
		"oid": "L",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["N", "O"]
	},
	{
		"oid": "M",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["P"]
	},
	{
		"oid": "N",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["Q"]
	},
	{
		"oid": "O",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["R"]
	},
	{
		"oid": "P",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP",
		"consumers": ["R"]
	},
	{
		"oid": "Q",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["S"]
	},
	{
		"oid": "R",
		"categoryType": "Application",
		"dropclass": "dlg.apps.simple.SleepAndCopyApp",
		"outputs": ["T"]
	},
	{
		"oid": "S",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP"
	},
	{
		"oid": "T",
		"categoryType": "data",
		"dropclass": "dlg.data.drops.memory.InMemoryDROP"
	}
]
