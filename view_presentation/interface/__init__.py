#code to import all interfaces by default
#This allows the user to not worry about importing any new interface
import os
for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    __import__('view_presentation.interface.'+module[:-3], locals(), globals())
del module