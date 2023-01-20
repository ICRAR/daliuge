from dlg.drop import BarrierAppDROP

##
# @brief Loop
# @details A loop placeholder drop
# @par EAGLE_START
# @param category Loop
# @param tag template
# @param appclass Application Class//String/ComponentParameter/readonly//False/False/Application class
# @param execution_time Execution Time//Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus No. of CPUs//Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start Group start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param num_of_iter No. of iterations/2/Integer/ComponentParameter/readwrite//False/False/Number of iterations
# @par EAGLE_END
class LoopDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a loop in the template palette
    """

    pass


##
# @brief MKN
# @details A MKN placeholder drop
# @par EAGLE_START
# @param category MKN
# @param tag template
# @param appclass Application Class//String/ComponentParameter/readonly//False/False/Application class
# @param execution_time Execution Time//Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus No. of CPUs//Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start Group start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @par EAGLE_END
class MKNDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a MKN in the template palette
    """

    pass


##
# @brief SubGraph
# @details A SubGraph placeholder drop
# @par EAGLE_START
# @param category SubGraph
# @param tag template
# @param appclass Application Class//String/ComponentParameter/readonly//False/False/Application class
# @param execution_time Execution Time//Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus No. of CPUs//Integer/ComponentParameter/readonly//False/False/Number of cores used
# @par EAGLE_END
class SubGraphDrop(BarrierAppDROP):
    """
    This only exists to make sure we have a SubGraph in the template palette
    """

    pass


##
# @brief Comment
# @details A comment placeholder drop
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
# @details A loop placeholder drop
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
# @details An Exclusive Force Node placeholder
# @par EAGLE_START
# @param category ExclusiveForceNode
# @param tag template
# @par EAGLE_END
class ExclusiveForceDrop(BarrierAppDROP):
    """
    This only exists to make sure we have an exclusive force node in the template palette
    """

    pass
