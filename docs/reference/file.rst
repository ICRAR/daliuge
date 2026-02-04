.. _file_drop:

FileDROP 
########

Essential attributes
====================

- ``filepath`` this is an essential parameter if needing to use specific paths by the user. If left blank, DALiUGE will generate a file name. 

Replacement rules
=================
DALiuGE supports the the use of f-string-style replacements using brackets ("{}"). This is useful for situations where you may want to add prefixes or suffixes to pathnames. It is possible to use DALiuGE's auto-naming system with prefixes.


The DALiuGE Engine will auto-replace the following keywords: 

.. list-table:: 
   :header-rows: 1

   * - Keyword:
     - Replacement: 
     - Example use case:
   * - ``auto``
     - DROP UID + Human Readable Key
     - * Use when wanting file extensions 
       * E.g. ``{auto}_reduced.ms`` will 
       * Useful in loops when you want some control over the filename
   *  - ``datetime``
      - Replaces with "%Y-%m-%d" formatted data
      - Useful for scatters and long-running workflows
   *  - ``uid``
      - Generate a random UID 
      - Prototyping applications or data
   * - ``dlg``
     - 'DALiuGE' 
     - Prototyping applications or data

     
For example, using the filename `{dlg}_{auto}.txt` will produce `DALiuGE_2026-01-29T16_32_39_4_0.txt`. 

