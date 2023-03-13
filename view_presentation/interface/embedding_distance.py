import gensim
import view_presentation.config as config


#Class to calculate embedding based distance between text

class EmbeddingModel:
    def __init__(self):
        self.gensim_model = gensim.models.KeyedVectors.load_word2vec_format(config.gensim_path, binary=True)

    def get_distance (self,sentence1, sentence2):
        
        return self.gensim_model.wmdistance(sentence1.lower().split(),sentence2.lower().split())