from typing import List
import ipywidgets as widgets
from IPython.display import display, clear_output, HTML

import pandas as pd
import os

from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from algebra import API
from qbe_module.column_selection import ColumnSelection
from qbe_module.query_by_example import ExampleColumn, QueryByExample
from qbe_module.materializer import Materializer
from tqdm import tqdm

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class Demo:
    def __init__(self, graph_path='/home/cc/opendata_large_graph/', data_path='/home/cc/opendata_cleaned/'):

        # path to store the aurum graph index
        self.graph_path = graph_path
        # graph_path = '/home/cc/chicago_open_data_graph/'

        # path to store the raw data
        self.data_path = data_path
        # data_path = '/home/cc/chicago_open_data/'

        store_client = StoreHandler()
        network = fieldnetwork.deserialize_network(graph_path)
        aurum_api = API(network=network, store_client=store_client)

        # QBE interface
        self.qbe = QueryByExample(aurum_api)

        self.example_columns = []
        self.candidate_list = []

    def end_to_end_demo(self):

        self.view_specification()
        self.find_candidate_columns()

    def view_specification(self):

        global row_num
        global col_num
        row_num = 3
        col_num = 3

        default_values = [["school", "type", "rating"],
                          ["Ogden International High School", "IB", "Level 1"],
                          ["Hyde Park High School", "General Education", "Level 2"]]

        attr_style = "<style>.attr input { background-color:#D0F0D0 !important; }</style>"

        global x
        x = [[widgets.Text(value=default_values[i][j]) for j in range(col_num)] for i in range(row_num)]

        out = widgets.Output()

        button1 = widgets.Button(description="Add Column")
        button2 = widgets.Button(description="Remove Column")
        button3 = widgets.Button(description="Add Row")
        button4 = widgets.Button(description="Remove Row")
        button5 = widgets.Button(description="Confirm")

        @out.capture()
        def draw():
            attr_line = [widgets.HTML(attr_style)]
            attr_line.extend([item.add_class('attr') for item in x[0]])
            attrs = widgets.HBox(attr_line)
            display(widgets.VBox([attrs] + [widgets.HBox(x[i]) for i in range(1, row_num)]))
            # display(widgets.VBox([widgets.HBox(x[i]) for i in range(1, row_num)]))

        @out.capture()
        def display_all():
            display(widgets.HBox([button1, button2, button3, button4]))
            draw()
            # display(out)
            display(button5)

        @out.capture()
        def add_column(_):
            global col_num
            global x
            # global out
            col_num += 1
            x[0].append(widgets.Text(value="col" + str(len(x[0]))))
            for i in range(1, row_num):
                x[i].append(widgets.Text())

            out.clear_output()
            with out:
                display_all()

        @out.capture()
        def remove_column(_):
            global col_num
            global x
            # global out
            for i in range(row_num):
                w = x[i].pop()
                w.close()
            col_num -= 1
            out.clear_output()
            with out:
                display_all()

        @out.capture()
        def add_row(_):
            global row_num
            global x
            # global out
            row_num += 1
            x.append([widgets.Text() for _ in range(col_num)])
            out.clear_output()
            with out:
                display_all()

        @out.capture()
        def remove_row(_):
            global row_num
            global x
            # global out
            ws = x.pop()
            for w in ws:
                w.close()
            row_num -= 1
            out.clear_output()
            with out:
                display_all()

        def confirm(_):
            # global values
            # global attrs
            # global example_columns

            values = []
            attrs = []
            candidate_list = [[] for _ in range(col_num)]

            for i in range(row_num):
                row = []
                for j in range(col_num):
                    if i == 0:
                        attrs.append(x[i][j].value)
                    else:
                        row.append(x[i][j].value)
                if i != 0:
                    values.append(row)

            example_columns = []
            for i, attr in enumerate(attrs):
                example_col = ExampleColumn(attr, [values[j][i] for j in range(row_num - 1)])
                example_columns.append(example_col)

            self.example_columns = example_columns
            print("Confirmed")
            # self.get_relevant_columns(example_columns)


        button1.on_click(add_column)
        button2.on_click(remove_column)
        button3.on_click(add_row)
        button4.on_click(remove_row)
        button5.on_click(confirm)

        display_all()

    def find_candidate_columns(self):
        self.candidate_list = self.qbe.find_candidate_columns(self.example_columns, cluster_prune=True)

        for i, candidate in enumerate(self.candidate_list):
            print('\ncolumn {}: found {} candidate columns'.format(self.example_columns[i].attr, len(candidate)))
            for c in candidate:
                print(c)

    def show_candidate_columns(self):

        if len(self.candidate_list) == 0:
            print("No candidate columns to show")
            return

        output = widgets.Output()

        default_col = self.example_columns[0].attr

        candidate_columns_dict = {}
        for i, candidate in enumerate(self.candidate_list):
            candidate_columns_dict[self.example_columns[i].attr] = candidate

        def highlight_cols(s, color='lightgreen'):
            return 'background-color: %s' % color

        def apply_highlight(df, cols_to_highlight, color='lightyellow'):
            html = df.style.applymap(highlight_cols,
                                     subset=pd.IndexSlice[:, cols_to_highlight],
                                     color=color).render()
            return html

        def display_candidate(candidate, dropdown_candidate, num=10):
            output.clear_output()

            #     global dropdown_candidate

            with output:
                #         print(dropdown_candidate.options)
                print("View Candidate Columns")
                display(dropdown_col, dropdown_candidate, bounded_num)

            split = candidate.rsplit(".", 1)
            table_name, col_name = split[0], split[1]
            with output:
                print("candidate table column:", table_name, col_name)

            df = pd.read_csv(os.path.join(self.data_path, table_name))
            df = df.head(num)
            cols_to_highlight = [col_name]

            html = apply_highlight(df, cols_to_highlight)

            with output:
                display(HTML(html))

        def dropdown_col_eventhandler(change):
            col = change.new
            #     global dropdown_candidate
            #     dropdown_candidate.options = list(candidate_columns_dict[col])
            new_dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
                                                      options=list(candidate_columns_dict[col]))
            dropdown_candidate = new_dropdown_candidate
            dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')

            num = int(bounded_num.value)

            display_candidate(dropdown_candidate.options[0], dropdown_candidate, num=num)

        def dropdown_candidate_eventhandler(change):
            candidate = change.new

            #     col = dropdown_col.value
            #     global dropdown_candidate
            #     new_dropdown_candidate = widgets.Dropdown(description="Candidate Table Column", options=list(
            #     candidate_columns_dict[col]))
            #     dropdown_candidate = new_dropdown_candidate

            num = int(bounded_num.value)

            dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')
            display_candidate(candidate, dropdown_candidate, num=num)

        def bounded_num_eventhandler(change):
            num = int(change.new)

            col = dropdown_col.value
            new_dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
                                                      options=list(candidate_columns_dict[col]))
            dropdown_candidate = new_dropdown_candidate
            dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')

            candidate = dropdown_candidate.value

            display_candidate(candidate, dropdown_candidate, num=num)

        dropdown_col = widgets.Dropdown(options=list(candidate_columns_dict.keys()), description="Column Name")
        dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
                                              options=candidate_columns_dict[default_col])
        bounded_num = widgets.BoundedFloatText(min=0, max=1000, value=10, step=1, description="Size")

        dropdown_col.observe(dropdown_col_eventhandler, names='value')
        dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')
        bounded_num.observe(bounded_num_eventhandler, names='value')

        print("View Candidate Columns")

        display(dropdown_col, dropdown_candidate, bounded_num)

        display(output)

