import pandas as pd
import embedding_distance

class interface:
    def __init__(self,name):
        self.name=name
        self.asked_questions={}
