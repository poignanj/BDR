import json
import sys
import re

help = """
    USAGE : pathSearch 
    [-n"SpellName"] One part of the spell name or the whole thing. 
    [-cV|S|M|FD] Spell components, concat for more than one (respect order)
    [-levelX] X is spell level, specific class coming later
    [-SRy|n] Spell Resistance yes / no
"""
if len(sys.argv) == 1:
    print(help)
else:
    if sys.argv[1] == "help":
        print(help)
    else:
        reg_spellName = re.compile(r"-n\"([^\"])+\"")
        reg_level = re.compile(r"-level([0-9]+)")
        reg_SR = re.compile(r"-SR(y|n)")
        reg_compo = re.compile(r"-c(V|S|M|(FD))+")

        def sortSpells(param):
            return 0



        def main(arguments):
            compo = list()
            level = -1
            spellName = ""
            SR = ""
            for arg in arguments:
                if reg_spellName.findall(arg):
                    
            return 0



        main(sys.argv)