from dlg.apps.app_base import BarrierAppDROP


##
# @brief Scatter
# @details A Scatter template drop
# @par EAGLE_START
# @param category Scatter
# @param tag template
# @param num_of_copies 4/Integer/ConstructParameter/NoPort/ReadWrite//False/False/Specifies the number of replications of the content of the scatter construct
# @par EAGLE_END
class ScatterDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a Scatter in the template palette
    """


##
# @brief Gather
# @details A Gather template drop
# @par EAGLE_START
# @param category Gather
# @param tag template
# @param num_of_inputs 2/Integer/ConstructParameter/NoPort/ReadWrite//False/False/Number of inputs
# @param gather_axis 0/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/Index of gather axis
# @par EAGLE_END
class GatherDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a GroupBy in the template palette
    """


##
# @brief Loop
# @details A loop template drop
# @par EAGLE_START
# @param category Loop
# @param tag template
# @param num_of_iter 2/Integer/ConstructParameter/NoPort/ReadWrite//False/False/Number of iterations
# @par EAGLE_END
class LoopDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a loop in the template palette
    """


##
# @brief MKN
# @details A MKN template drop
# @par EAGLE_START
# @param category MKN
# @param tag template
# @param k 1/Integer/ConstructParameter/NoPort/ReadWrite//False/False/Internal multiplicity
# @par EAGLE_END
class MKNDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a MKN in the template palette
    """


##
# @brief GroupBy
# @details A GroupBy template drop
# @par EAGLE_START
# @param category GroupBy
# @param tag template
# @param num_of_inputs 2/Integer/ConstructParameter/NoPort/ReadWrite//False/False/Number of inputs
# @param gather_axis 0/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/Index of gather axis
# @par EAGLE_END
class GroupByDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a GroupBy in the template palette
    """


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


##
# @brief Comment
# @details A comment template drop
# @par EAGLE_START
# @param category Comment
# @param dropclass dlg.apps.constructs.CommentDrop/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name constructs/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param tag template
# @par EAGLE_END
class CommentDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a comment in the template palette
    """


##
# @brief Exclusive Force Node
# @details An Exclusive Force Node
# @par EAGLE_START
# @param category ExclusiveForceNode
# @param dropclass dlg.apps.constructs.ExclusiveForceDrop/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name construct/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param tag template
# @par EAGLE_END
class ExclusiveForceDrop(BarrierAppDROP):
    """
    This only exists to make sure we have an exclusive force node in the template palette
    """
