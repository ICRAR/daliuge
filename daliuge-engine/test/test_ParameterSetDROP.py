import unittest
from dlg.drop import ParameterSetDROP
from dlg.droputils import allDropContents


class test_ParameterSetDROP(unittest.TestCase):
    kwargs = {
        "mode": None,
        "Cimager": 2,
        "StringParam": "param",
        "Boolparam": True,
        "config_data": "",
        "iid": -1,
        "rank": 0,
        "consumers": ["drop-1", "drop-2"],
    }

    def test_initialize(self):
        yanda_kwargs = dict(self.kwargs)
        yanda_kwargs["mode"] = "YANDA"

        yanda_parset = ParameterSetDROP(oid="a", uid="a", **yanda_kwargs)
        standard_parset = ParameterSetDROP(oid="b", uid="b", **self.kwargs)

        yanda_output = "Cimager=2\nStringParam=param\nBoolparam=True"
        standard_output = (
            "mode=None\n"
            + yanda_output
            + "\nconfig_data=\niid=-1\nrank=0\nconsumers=['drop-1', 'drop-2']"
        )

        yanda_parset.setCompleted()
        standard_parset.setCompleted()

        self.assertEqual(yanda_output, allDropContents(yanda_parset).decode("utf-8"))
        self.assertEqual(
            standard_output, allDropContents(standard_parset).decode("utf-8")
        )
