.. _app_drop_foundations: 


Best practices
==============

- What certain apps can/can't do 
- Link to reference material for each app
  
PyFuncApp
----------

- Function parameters and ports: It is essential that input ports that are parsing data as a parameter to a PyFuncApp are labelled the same as the function parameter. For example, if the function is ``my_string_reverse(text: str)``, DALiuGE requires the input port to:

    - be named ``text``,
    - have the port made an InputPort, and
    - Set as an Application parameter (default)