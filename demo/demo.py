import sys
  
# append the path of the parent directory
sys.path.append("..")

from typing import List
import ipywidgets as widgets
from IPython.display import display, clear_output, HTML, display_html, Markdown

import pandas as pd
import time
import os
from itertools import chain,cycle

# from knowledgerepr import fieldnetwork
# from modelstore.elasticstore import StoreHandler
# from algebra import API
# from qbe_module.column_selection import ColumnSelection
# from qbe_module.query_by_example import ExampleColumn, QueryByExample
# from qbe_module.materializer import Materializer
# from view_distillation.view_distillation import ViewDistillation
from view_presentation.ver_view_presentation import ViewPresentation


import config
from dindex_store.discovery_index import load_dindex
from aurum_api.algebra import AurumAPI
from qbe_module.query_by_example import ExampleColumn, QueryByExample
from qbe_module.materializer import Materializer
# from tqdm import tqdm
# import os
# import json
# from view_distillation import vd
from view_distillation.vd import ViewDistillation


from tqdm import tqdm

import fire

import warnings

import random
random.seed(0)

warnings.simplefilter(action='ignore', category=FutureWarning)


class Ver:
    def __init__(self, graph_path='/home/cc/chicago_open_data_graph/', data_path='./../demo_dataset/'):
        # path to store the aurum graph index
        # self.graph_path = graph_path
        # graph_path = '/home/cc/opendata_large_graph/'

        # path to store the raw data
        self.data_path = data_path
        # data_path = '/home/cc/opendata_cleaned/'

        # store_client = StoreHandler()
        # network = fieldnetwork.deserialize_network(graph_path)
        # aurum_api = API(network=network, store_client=store_client)

        # QBE interface
        # self.qbe = QueryByExample(aurum_api)

        cnf = {setting: getattr(config, setting) for setting in dir(config)
            if setting.islower() and len(setting) > 2 and setting[:2] != "__"}

        dindex = load_dindex(cnf)

        api = AurumAPI(dindex)

        # QBE interface
        self.qbe = QueryByExample(api)

        self.example_columns = None
        self.candidate_list = None
        self.join_graphs = None
        self.view_dfs = []
        self.view_names = []

        self.G = None
        
        # print("in")

    def view_specification(self, examples: List[str] = []):
        global row_num
        global col_num
        row_num = 3
        col_num = 3

        default_values = [["school name", "school type", "school day"],
                          ["Ogden International High School", "Charter", "HALF DAY"],
                          ["University of Chicago Woodlawn", "Neighborhood", "FULL DAY"]]

        if len(examples) > 0:
            row_num = len(examples)
            col_num = len(examples[0])

            default_values = [["" for j in range(col_num)] for i in range(row_num)]
            for i in range(row_num):
                for j in range(col_num):
                    default_values[i][j] = examples[i][j]

        attr_style = "<style>.attr input { background-color:#D0F0D0 !important; }</style>"
        x = [[widgets.Text(value=default_values[i][j]) for j in range(col_num)] for i in range(row_num)]

        out = widgets.Output()

        @out.capture()
        def draw():
            attr_line = [widgets.HTML(attr_style)]
            attr_line.extend([item.add_class('attr') for item in x[0]])
            attrs = widgets.HBox(attr_line)
            display(widgets.VBox([attrs] + [widgets.HBox(x[i]) for i in range(1, row_num)]))
            # display(widgets.VBox([widgets.HBox(x[i]) for i in range(1, row_num)]))


        @out.capture()
        def add_column(_):
            global col_num
            col_num += 1
            x[0].append(widgets.Text(value="col" + str(len(x[0]))))
            for i in range(1, row_num):
                x[i].append(widgets.Text())
            clear_output()
            draw()


        @out.capture()
        def remove_column(_):
            global col_num
            for i in range(row_num):
                w = x[i].pop()
                w.close()
            col_num -= 1
            clear_output()
            draw()


        @out.capture()
        def add_row(_):
            global row_num
            row_num += 1
            x.append([widgets.Text() for _ in range(col_num)])
            clear_output()
            draw()


        @out.capture()
        def remove_row(_):
            global row_num
            ws = x.pop()
            for w in ws:
                w.close()
            row_num -= 1
            clear_output()
            draw()


        def confirm(_):
            global values
            global attrs
            values = []
            attrs = []
            self.candidate_list = [[] for _ in range(col_num)]
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
                example_col = ExampleColumn(attr, [values[j][i] for j in range(row_num-1)])
                example_columns.append(example_col)

            self.example_columns = example_columns
            # print("Confirmed")

            print("---------------------------------------")

            self.find_candidate_columns()

            self.find_join_graphs()

            self.materialize_join_graphs()

            # self.view_distillation()

            self.show_views()

            # self.view_presentation()


        button1 = widgets.Button(description="Add Column")
        button2 = widgets.Button(description="Remove Column")
        button3 = widgets.Button(description="Add Row")
        button4 = widgets.Button(description="Remove Row")
        button5 = widgets.Button(description="Find\nViews")
        button1.on_click(add_column)
        button2.on_click(remove_column)
        button3.on_click(add_row)
        button4.on_click(remove_row)
        button5.on_click(confirm)

        # print("Query-by-Example interface")
        display(Markdown('<h1><center><strong>{}</strong></center></h1>'.format("Ver QBE interface")))

        display(widgets.HBox([button1, button2, button3, button4]))
        draw()
        display(out)
        display(button5)

    def view_specification_archived(self, *examples, **attrs):

        # import ast

        if "attrs" not in attrs.keys():
            attrs["attrs"] = ["" for i in range(len(examples[0]))]

        # new_examples = []
        # for example in examples:
        #     example = ast.literal_eval(example)
        #     example = [n.strip() for n in example]
        #     new_examples.append(example)
        # examples = new_examples

        # print(examples)
        # print(attrs)

        example_columns = []
        for i, attr in enumerate(attrs["attrs"]):
            example_col = ExampleColumn(attr, [examples[j][i] for j in range(len(examples))])
            example_columns.append(example_col)

        self.example_columns = example_columns

        # for example_column in example_columns:
        #     print(example_column.attr)
        #     print(example_column.examples)

        return self

    def find_candidate_columns(self):

        # print("---------------------------------------")

        print("Finding candidate columns...")

        self.candidate_list = self.qbe.find_candidate_columns(self.example_columns, cluster_prune=True)

        # self.candidate_list = [x[:15] for x in self.candidate_list]

        # for i, candidate in enumerate(self.candidate_list):
        #     print('Column {}: found {} candidate columns'.format(self.example_columns[i].attr, len(candidate)))
        #     for c in candidate:
        #         print(c.to_str())

        print("---------------------------------------")

        return self

    def find_join_graphs(self):

        # print("---------------------------------------")

        print("Finding join graphs among candidate columns...")

        cand_groups, self.tbl_cols = self.qbe.find_candidate_groups(self.candidate_list)
        self.join_graphs = self.qbe.find_join_graphs_for_cand_groups(cand_groups)

        # self.join_graphs = self.qbe.find_join_graphs_between_candidate_columns(self.candidate_list, order_chain_only=True)
        
        # self.join_graphs = self.join_graphs[:5]

        # print("found {} join graphs".format(len(self.join_graphs)))
        
        # for i, join_graph in enumerate(self.join_graphs[:10]):
        #     print("----join graph {}----".format(i))
        #     join_graph.display()

        print("---------------------------------------")

        return self

    def materialize_join_graphs(self, dir_path=None):

        # print("---------------------------------------")

        # print("Materializing join graphs...")

        # materializer = Materializer(self.data_path, 200)


        # j = 0
        # for join_graph in self.join_graphs:

        #     #ca join graph can produce multiple views because different columns are projected
        #     df_list = materializer.materialize_join_graph(join_graph)

        #     for df in df_list:

        #         if len(df) != 0:
        #             j += 1
        #             # print("non empty view", j)
        #             new_cols = []
        #             k = 1
        #             for col in df.columns:
        #                 new_col = col.split(".")[-1]
        #                 if new_col in new_cols:
        #                     new_col += str(k)
        #                     k += 1
        #                 new_cols.append(new_col)
        #             df.columns = new_cols
        #             self.view_dfs.append(df)

        #             if dir_path is not None:
        #                 df.to_csv(f"{dir_path}/view{j}.csv", index=False)


        # print(f"Materialized {len(self.view_dfs)} non-empty views")
        print("---------------------------------------")

        # output_path = './output/'  # path to store the output views
        sep = ','  # csv separator

        # if not os.path.exists(output_path):
        #     os.makedirs(output_path)
            
        materializer = Materializer(self.data_path, self.tbl_cols, 200, sep)

        j = 0
        for join_graph in tqdm(self.join_graphs):
            """
            a join graph can produce multiple views because different columns are projected
            """
            df_list = materializer.materialize_join_graph(join_graph)
            for df in df_list:
                if len(df) != 0:
                    metadata = {}
                    metadata["join_graph"] = join_graph.to_str()
                    metadata["columns_proj"] = list(df.columns)
                    # with open(f"./{output_path}/view{j}.json", "w") as outfile:
                    #     json.dump(metadata, outfile)
                    j += 1
                    # print("non empty view", j)
                    new_cols = []
                    k = 1
                    for col in df.columns:
                        new_col = col.split(".")[-1]
                        if new_col in new_cols:
                            new_col += str(k)
                            k += 1
                        new_cols.append(new_col)
                    df.columns = new_cols
                    # df.to_csv(f"./{output_path}/view{j}.csv", index=False)

                    self.view_dfs.append(df)

        print(f"Materialized {len(self.view_dfs)} non-empty views")

        print("---------------------------------------")

        return self

    
    def view_distillation(self, remove_identical_views=True,
                                remove_contained_views=True,
                                union_complementary_views=True,
                                graph=False,
                                path_to_views=None):
        
        # print("---------------------------------------")

        print("Distilling views")

        if path_to_views is None:
            self.vd = ViewDistillation(dfs=self.view_dfs)
        else:
            self.vd = ViewDistillation(path_to_views=path_to_views)

        if graph:
            self.G = self.vd.prune_graph(remove_identical_views,
                                        remove_contained_views,
                                        union_complementary_views)
        else:
            self.vd.distill_views(remove_identical_views,
                                  remove_contained_views,
                                  union_complementary_views)
        
        current_views = self.vd.get_current_views()
        self.view_names = current_views
        self.view_dfs = self.vd.get_dfs(current_views)

        print("---------------------------------------")

        return self

    # def view_presentation(self):

    #     button = widgets.Button(description="view presentation")
    #     output = widgets.Output()

    #     # display(button, output)

    #     def view_presentation(b):
    #         with output:
    #             print("view presentation")

    #     button.on_click(view_presentation)
    #     display(button)
    #     display(output)

    def _apply_highlight(self, df, cols_to_highlight, color='lightyellow'):
            
            def highlight_cols(s, color='lightgreen'):
                return 'background-color: %s' % color
            
            html = df.style.applymap(highlight_cols,
                                     subset=pd.IndexSlice[:, cols_to_highlight],
                                     color=color).render()
            return html

    def show_candidate_columns(self):

        if len(self.candidate_list) == 0:
            print("No candidate columns to show")
            return

        output = widgets.Output()

        default_col = self.example_columns[0].attr

        candidate_columns_dict = {}
        for i, col_candidates in enumerate(self.candidate_list):
            col_candidates_new = []
            for col in col_candidates:
                col_candidates_new.append(f"{col.tbl_name},{col.attr_name}")
            candidate_columns_dict[self.example_columns[i].attr] = col_candidates_new

        def highlight_cols(s, color='lightgreen'):
            return 'background-color: %s' % color


        def display_candidate(candidate, dropdown_candidate, num=10):
            output.clear_output()

            #     global dropdown_candidate

            with output:
                #         print(dropdown_candidate.options)
                print("View Candidate Columns")
                display(dropdown_col, dropdown_candidate, bounded_num)

            split = candidate.rsplit(",", 1)
            table_name, col_name = split[0], split[1]
            with output:
                print("candidate table and column:", table_name, col_name)

            df = pd.read_csv(os.path.join(self.data_path, table_name))
            df = df.head(num)
            cols_to_highlight = [col_name]

            html = self._apply_highlight(df, cols_to_highlight)

            with output:
                display(HTML(html))

        def dropdown_col_eventhandler(change):
            col = change.new
            #     global dropdown_candidate
            dropdown_candidate.options = list(candidate_columns_dict[col])
            # new_dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
            #                                           options=list(candidate_columns_dict[col]))
            # dropdown_candidate = new_dropdown_candidate
            # dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')

            num = int(bounded_num.value)

            display_candidate(dropdown_candidate.options[0], dropdown_candidate, num=num)

        def dropdown_candidate_eventhandler(change):
            candidate = change.new

            num = int(bounded_num.value)

            col = dropdown_col.value
            dropdown_candidate.options = list(candidate_columns_dict[col])

            # new_dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
            #                                           options=list(candidate_columns_dict[col]))
            # dropdown_candidate = new_dropdown_candidate
            # dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')

            display_candidate(candidate, dropdown_candidate, num=num)

        def bounded_num_eventhandler(change):
            num = int(change.new)

            col = dropdown_col.value            
            dropdown_candidate.options = list(candidate_columns_dict[col])

            # new_dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
            #                                           options=list(candidate_columns_dict[col]))
            # dropdown_candidate = new_dropdown_candidate
            # dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')

            candidate = dropdown_candidate.value

            display_candidate(candidate, dropdown_candidate, num=num)

        dropdown_col = widgets.Dropdown(options=list(candidate_columns_dict.keys()), description="Column Name")
        dropdown_candidate = widgets.Dropdown(description="Candidate Table Column",
                                              options=candidate_columns_dict[default_col])
        bounded_num = widgets.BoundedFloatText(min=0, max=1000, value=10, step=1, description="Size")

        dropdown_col.observe(dropdown_col_eventhandler, names='value')
        dropdown_candidate.observe(dropdown_candidate_eventhandler, names='value')
        bounded_num.observe(bounded_num_eventhandler, names='value')

        with output:
            print("View Candidate Columns")
            display(dropdown_col, dropdown_candidate, bounded_num)

        display(output)

    def _apply_highlight_multi(self, df, cols_to_highlight):
            
            def highlight_cols(s, color):
                return 'background-color: %s' % color
            
            s = df.style
            
            for col, color in cols_to_highlight:
                s = s.applymap(highlight_cols,
                                subset=pd.IndexSlice[:, [col]],
                                color=color)
            
            html = s.render()
            return html


    def show_join_graphs(self):
        
        output = widgets.Output()


        def display_side_by_side(dfs,titles=cycle(['']), join_keys=None):
            html_str=''
            i = 0
            for df, title in zip(dfs, chain(titles,cycle(['</br>'])) ):
                html_str += '<th style="text-align:center"><td style="vertical-align:top">'
                html_str += f'<h2 style="text-align:center;">{title}</h2>'

                df_html = df.to_html()
                if len(join_keys) > 0:
                    # print(df.columns, cols_to_highlight[i])
                    df_html = self._apply_highlight_multi(df, join_keys[i])
                    i+=1

                html_str += df_html.replace('table','table style="display:inline"')
                html_str += '</td></th>'
            display_html(html_str,raw=True)

        def display_join_graph(join_graph_idx, num=10):

            output.clear_output()

        #     global dropdown_candidate

            with output:
        #         print(dropdown_candidate.options)
                print("View Join Graphs")
                display(dropdown_idx)
                display(bounded_num)

            dfs = []
            # titles = join_graphs[join_graph_idx]
            join_graph = self.join_graphs[join_graph_idx]

            from collections import defaultdict
            # d = defaultdict(lambda: defaultdict(set))
            d = defaultdict(lambda: defaultdict(list))

            colors = ["lightgreen", "lightyellow", "lightblue", "lightpink"]
            color_idx = 0

            for edge, path in join_graph.graph_dict.items():
                
                for i, join_key_pair in enumerate(path.path):
                    
                    if len(join_key_pair[0].field_name) > 0:
                        # d[join_key_pair[0].source_name]["join_key"].add(join_key_pair[0].field_name)
                        d[join_key_pair[0].source_name]["join_key"].append((join_key_pair[0].field_name, colors[color_idx]))
                                        
                    if len(join_key_pair[1].field_name) > 0:
                        # d[join_key_pair[1].source_name]["join_key"].add(join_key_pair[1].field_name)
                        d[join_key_pair[1].source_name]["join_key"].append((join_key_pair[1].field_name, colors[color_idx]))

                        color_idx += 1
                        if color_idx >= len(colors):
                            color_idx = 0

                for i, attrs in enumerate(path.tbl_proj_attrs):
                    for attr in attrs:
                        # d[attr.tbl_name]["attrs_to_project"].add(attr.attr_name)
                        d[attr.tbl_name]["attrs_to_project"].append(attr.attr_name)

            join_keys = []

            # print(d)

            for table_name, d1 in d.items():

                # join_key = list(d1["join_key"])
                join_key = set(t[0] for t in d1["join_key"])
                # join_keys.append(join_key)
                join_keys.append(d1["join_key"])

                attrs_to_project = set(d1["attrs_to_project"])
                
                cols = attrs_to_project.copy()
                cols.update(join_key)

                # print(cols)
                
                df = pd.read_csv(os.path.join(self.data_path, table_name))[list(cols)]
                
                if isinstance(df, pd.Series):
                    df.to_frame()
                    
                df = df.head(num)

                dfs.append(df)

            with output:
                # print(join_keys)
                display_side_by_side(dfs, list(d.keys()), join_keys)

        def dropdown_idx_eventhandler(change):
            idx = int(change.new)
            num = int(bounded_num.value)
            display_join_graph(idx, num)

        def bounded_num_eventhandler(change):
            num = int(change.new)
            idx = dropdown_idx.value
            display_join_graph(idx, num)


        dropdown_idx = widgets.Dropdown(options=[i for i in range(len(self.join_graphs))], description="Idx")
        bounded_num = widgets.BoundedFloatText(min=0, max=1000, value=10, step=1, description="Size")

        dropdown_idx.observe(dropdown_idx_eventhandler, names='value')
        bounded_num.observe(bounded_num_eventhandler, names='value')

        with output:
            print("View Join Graphs")
            display(dropdown_idx)
            display(bounded_num)

        display(output)


    def show_views(self):
    

        output = widgets.Output()

        # back here
        @output.capture()
        def display_query():
            col_num = len(self.example_columns) # 3
            row_num = max([len(self.example_columns[i].examples) for i in range(col_num)]) + 1 # 2 + 1
            examples = [[None] * col_num for _ in range(row_num)]

            for i in range(col_num):
                examples[0][i] = self.example_columns[i].attr
            
            for i, example_column in enumerate(self.example_columns):
                for j in range(len(example_column.examples)):
                    examples[j+1][i] = example_column.examples[j]

            self.view_specification(examples)

        def display_view(view_idx, num=10):

            output.clear_output()

        #     global dropdown_candidate

            with output:
                display_query()
        #         print(dropdown_candidate.options)
                print("Show Views")
                display(dropdown_view)
                display(bounded_num)

            if view_idx != "":

                df = self.view_dfs[view_idx].head(num)

                with output:
                    display(df)

        def dropdown_view_eventhandler(change):
            idx = int(change.new)
            num = int(bounded_num.value)
            display_view(idx, num)

        def bounded_num_eventhandler(change):
            num = int(change.new)
            idx = dropdown_view.value
            display_view(idx, num)

        options = [""] + [i for i in range(len(self.view_dfs))]

        dropdown_view = widgets.Dropdown(options=options, description="View")
        bounded_num = widgets.BoundedFloatText(min=0, max=1000, value=10, step=1, description="Size")

        dropdown_view.observe(dropdown_view_eventhandler, names='value')
        bounded_num.observe(bounded_num_eventhandler, names='value')

        with output:
            print("Show Views")
            display(dropdown_view, bounded_num)

        display(output)

    def view_presentation(self):

        query = " ".join([ex.attr for ex in self.example_columns])

        vp = ViewPresentation(query, self.vd)
        
        vp.choose_interface()


if __name__ == '__main__':
  fire.Fire(Ver)
