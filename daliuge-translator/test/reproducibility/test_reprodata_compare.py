import unittest

from dlg.common.reproducibility.reprodata_compare import (
    compare_signatures,
    generate_comparison,
    process_single,
    process_multi,
    is_single,
)


class GenerateReprodataSummaryTest(unittest.TestCase):
    def test_is_single(self):
        multi_dict = {"rmode": "-1"}
        single_dict = {"rmode": "2"}
        self.assertFalse(is_single(multi_dict))
        self.assertTrue(is_single(single_dict))  # add assertion here

    def test_process_single(self):
        single_dict = {"rmode": "1", "signature": "1234"}
        wrong_rmode_dict = {}
        no_signature_dict = {"rmode": "1"}
        self.assertEqual({1: "1234"}, process_single(single_dict))
        self.assertEqual({0: None}, process_single(wrong_rmode_dict))
        self.assertEqual({1: None}, process_single(no_signature_dict))

    def test_process_multi(self):
        complete_dict = {
            "rmode": "-1",
            "RERUN": {
                "signature": "5638c2fa8e40d597153716357412e451d7734c8b36626cac3873a69f62510"
            },
            "REPEAT": {
                "signature": "492ae26c1f505b32a2aad69c28a2b91921ee92019b2a70e2459dff2d9029"
            },
            "RECOMPUTE": {
                "signature": "4e69f408d932e7c52bfcff4c5fc4e2e4626d4646346717f57e18d4b07"
            },
            "REPRODUCE": {
                "signature": "6481936054065fbcb04626ea70afbc0ae6a8af8f8b38b30074f53ecae"
            },
            "REPLICATE_SCI": {
                "signature": "e5d63312073ade0f606f9a02214c9cbaa0eba7f3a35e7dc1e78ce"
            },
            "REPLICATE_COMP": {
                "signature": "eb84113bfa4ef02a68cfcc20ef0d7dd2d2bdd7b2db33c4aade8f"
            },
            "REPLICATE_TOTAL": {
                "signature": "a9b1b29fbf6e44cd368abd3775f6d48d44aceee93d07a8b257da"
            },
        }
        incomplete_dict = {
            "rmode": "-1",
            "RERUN": {
                "signature": "5638c2fa8e40d597153716357412e451d7734c8b36626cac3873a69f62510"
            },
            "REPEAT": {
                "signature": "492ae26c1f505b32a2aad69c28a2b91921ee92019b2a70e2459dff2d9029"
            },
            "RECOMPUTE": {
                "signature": "4e69f408d932e7c52bfcff4c5fc4e2e4626d4646346717f57e18d4b07"
            },
            "REPRODUCE": {
                "signature": "6481936054065fbcb04626ea70afbc0ae6a8af8f8b38b30074f53ecae"
            },
        }
        empty_dict = {
            "rmode": "-1",
        }
        self.assertEqual(
            {
                1: "5638c2fa8e40d597153716357412e451d7734c8b36626cac3873a69f62510",
                2: "492ae26c1f505b32a2aad69c28a2b91921ee92019b2a70e2459dff2d9029",
                4: "4e69f408d932e7c52bfcff4c5fc4e2e4626d4646346717f57e18d4b07",
                5: "6481936054065fbcb04626ea70afbc0ae6a8af8f8b38b30074f53ecae",
                6: "e5d63312073ade0f606f9a02214c9cbaa0eba7f3a35e7dc1e78ce",
                7: "eb84113bfa4ef02a68cfcc20ef0d7dd2d2bdd7b2db33c4aade8f",
                8: "a9b1b29fbf6e44cd368abd3775f6d48d44aceee93d07a8b257da",
            },
            process_multi(complete_dict),
        )
        self.assertEqual(
            {
                1: "5638c2fa8e40d597153716357412e451d7734c8b36626cac3873a69f62510",
                2: "492ae26c1f505b32a2aad69c28a2b91921ee92019b2a70e2459dff2d9029",
                4: "4e69f408d932e7c52bfcff4c5fc4e2e4626d4646346717f57e18d4b07",
                5: "6481936054065fbcb04626ea70afbc0ae6a8af8f8b38b30074f53ecae",
                6: None,
                7: None,
                8: None,
            },
            process_multi(incomplete_dict),
        )
        self.assertEqual(
            {1: None, 2: None, 4: None, 5: None, 6: None, 7: None, 8: None},
            process_multi(empty_dict),
        )


class CompareReprodataSummaryTest(unittest.TestCase):
    def test_generate_comparison(self):
        full_dict = {
            1: "5638c2fa8e40d597153716357412e451d7734c8b36626cac3873a69f62510",
            2: "492ae26c1f505b32a2aad69c28a2b91921ee92019b2a70e2459dff2d9029",
            4: "4e69f408d932e7c52bfcff4c5fc4e2e4626d4646346717f57e18d4b07",
            5: "6481936054065fbcb04626ea70afbc0ae6a8af8f8b38b30074f53ecae",
            6: "e5d63312073ade0f606f9a02214c9cbaa0eba7f3a35e7dc1e78ce",
            7: "eb84113bfa4ef02a68cfcc20ef0d7dd2d2bdd7b2db33c4aade8f",
            8: "a9b1b29fbf6e44cd368abd3775f6d48d44aceee93d07a8b257da",
        }
        empty_dict = {}
        semi_dict = {
            1: "5638c2fa8e40d597153716357412e451d7734c8b36626cac3873a69f62510",
            2: "492ae26c1f505b32a2aad69c28a2b91921ee92019b2a70e2459dff2d9029",
            4: "4e69f408d932e7c52bfcff4c5fc4e2e4626d4646346717f57e18d4b07",
            5: "6481936054065fbcb04626ea70afbc0ae6a8af8f8b38b30074f53ecae",
        }
        data_single = {"first": full_dict}
        data_three = {"first": full_dict, "second": empty_dict, "third": semi_dict}
        self.assertEqual({}, generate_comparison(data_single))
        self.assertEqual(
            {
                "first:second": {
                    1: False,
                    2: False,
                    4: False,
                    5: False,
                    6: False,
                    7: False,
                    8: False,
                },
                "first:third": {
                    1: True,
                    2: True,
                    4: True,
                    5: True,
                    6: False,
                    7: False,
                    8: False,
                },
                "second:third": {
                    1: False,
                    2: False,
                    4: False,
                    5: False,
                    6: False,
                    7: False,
                    8: False,
                },
            },
            generate_comparison(data_three),
        )

    def test_compare_signatures(self):
        first_dict = {1: "abc", 2: "123"}
        second_dict = {1: "abc", 2: "def"}
        third_dict = {}
        self.assertEqual(
            {1: True, 2: False, 4: False, 5: False, 6: False, 7: False, 8: False},
            compare_signatures(first_dict, second_dict),
        )
        self.assertEqual(
            {1: False, 2: False, 4: False, 5: False, 6: False, 7: False, 8: False},
            compare_signatures(third_dict, third_dict),
        )
        self.assertEqual(
            {1: False, 2: False, 4: False, 5: False, 6: False, 7: False, 8: False},
            compare_signatures(third_dict, second_dict),
        )
