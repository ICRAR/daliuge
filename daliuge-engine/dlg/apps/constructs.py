from dlg.apps.app_base import BarrierAppDROP


##
# @brief Scatter
# @details A Scatter template drop
# @par EAGLE_START
# @param category Scatter
# @param tag template
# @param num_of_copies Scatter dimension/4/Integer/ComponentParameter/readwrite//False/False/Specifies the number of replications of the content of the scatter construct
# @par EAGLE_END
class ScatterDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a Scatter in the template palette
    """

    pass


##
# @brief Gather
# @details A Gather template drop
# @par EAGLE_START
# @param category Gather
# @param tag template
# @param num_of_inputs No. of inputs/2/Integer/ApplicationArgument/readwrite//False/False/Number of inputs
# @param gather_axis Index of gather axis/0/Integer/ApplicationArgument/readwrite//False/False/Index of gather axis
# @par EAGLE_END
class GatherDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a GroupBy in the template palette
    """

    pass


##
# @brief Loop
# @details A loop template drop
# @par EAGLE_START
# @param category Loop
# @param tag template
# @param num_of_iter No. of iterations/2/Integer/ApplicationArguments/readwrite//False/False/Number of iterations
# @par EAGLE_END
class LoopDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a loop in the template palette
    """

    pass


##
# @brief MKN
# @details A MKN template drop
# @par EAGLE_START
# @param category MKN
# @param tag template
# @param k K/1/Integer/ApplicationArgument/readwrite//False/False/Internal multiplicity
# @par EAGLE_END
class MKNDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a MKN in the template palette
    """

    pass


##
# @brief GroupBy
# @details A GroupBy template drop
# @par EAGLE_START
# @param category GroupBy
# @param tag template
# @param num_of_inputs No. of inputs/2/Integer/ApplicationArgument/readwrite//False/False/Number of inputs
# @param gather_axis Index of gather axis/0/Integer/ApplicationArgument/readwrite//False/False/Index of gather axis
# @par EAGLE_END
class GroupByDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a GroupBy in the template palette
    """

    pass


##
# @brief SubGraph
# @details A SubGraph template drop
# @par EAGLE_START
# @param category SubGraph
# @param tag template
# @par EAGLE_END
class SubGraphDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a SubGraph in the template palette
    """

    pass


##
# @brief Comment
# @details A comment template drop
# @par EAGLE_START
# @param category Comment
# @param tag template
# @par EAGLE_END
class CommentDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a comment in the template palette
    """

    pass


##
# @brief Description
# @details A loop template drop
# @par EAGLE_START
# @param category Description
# @param tag template
# @par EAGLE_END
class DescriptionDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a description in the template palette
    """

    pass


##
# @brief Exclusive Force Node
# @details An Exclusive Force Node
# @par EAGLE_START
# @param category ExclusiveForceNode
# @param tag template
# @par EAGLE_END
class ExclusiveForceDrop(BarrierAppDROP):
    """
    This only exists to make sure we have an exclusive force node in the template palette
    """

    pass
