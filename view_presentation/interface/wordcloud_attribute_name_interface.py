from view_presentation.interface.interface import interface
import pandas as pd

class WordCloudAttributeNameInterface(interface):
    def __init__(self,name):
        self.name=name
        self.asked_questions={}
        self.curr_question_iter=0
    