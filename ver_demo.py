from demo.kw_search import search_attribute, search_content
import ipywidgets as widgets
from demo.kw_search import search_attribute, search_content
from IPython.display import display, clear_output
from itables import init_notebook_mode
import pandas as pd
from itables import show
from qbe_module.column import Column
from qbe_module.query_by_example import QueryByExample, ExampleColumn
from qbe_module.materializer import Materializer
from IPython.display import display, clear_output, HTML
from tabulate import tabulate

class Colors:
    CGREYBG = '\33[100m'
    CREDBG2 = '\33[101m'
    CGREENBG2 = '\33[102m'
    CYELLOWBG2 = '\33[103m'
    CBLUEBG2 = '\33[104m'
    CVIOLETBG2 = '\33[105m'
    CBEIGEBG2 = '\33[106m'
    CWHITEBG2 = '\33[107m'
    CGREENBG = '\33[42m'
    CBEIGEBG = '\33[46m'
    CBOLD = '\33[1m'
    CEND = '\33[0m'
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Demo:
    def __init__(self, aurum_api):
        self.aurum_api = aurum_api
        self.qbe = QueryByExample(aurum_api)
        self.candidate_list = []
        self.example_columns = []
        self.tbl_path = '/home/cc/adventureWork/'
        self.sample_size = 200

    def keyword_search(self):
        init_notebook_mode(all_interactive=True)

        # define widgets
        output = widgets.Output()
        @output.capture()
        def click1(h):
            with output:
                clear_output()
            res = search_attribute(self.aurum_api, kw.value, 100)
            data = {"table id": [], "attribute": []}
            for hit in res:
                data["table id"].append(hit.source_name)
                data["attribute"].append(hit.field_name)
            df = pd.DataFrame(data)
            show(df)

        @output.capture()
        def click2(h):
            with output:
                clear_output()
            res = search_content(self.aurum_api, kw.value, 100)
            data = {"table id": [], "attribute": []}
            for hit in res:
                data["table id"].append(hit.source_name)
                data["attribute"].append(hit.field_name)
            df = pd.DataFrame(data)
            show(df)

        kw = widgets.Text(
                placeholder="name",
                description='keyword',
                disabled=False)

        one = widgets.Button(description="search")
        one.on_click(click1)

        two = widgets.Button(description="search")
        two.on_click(click2)

        lst = ['Search Attribute', 'Search Content']
        btn = [one, two]

        list_widgets  = [
            widgets.HBox([kw,
            btn[num]]) for num, name in enumerate(lst)]

        children=list_widgets

        tab = widgets.Tab(children)
        [tab.set_title(num,name) for num, name in enumerate(lst)]

        display(tab)
        display(output)

    def qbe_interface(self):
        global row_num
        global col_num
        row_num = 3
        col_num = 3

        # default_values = [["", "", ""], ["United States", "USD", "US Dollar"], ["China", "CNY", "Yuan Renminbi"]]
        default_values = [["", "", ""], ["", "", ""], ["", "", ""]]

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
            for i in range(row_num):
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
            print("Confirmed")
            # self.get_relevant_columns(example_columns)


        button1 = widgets.Button(description="Add Column")
        button2 = widgets.Button(description="Remove Column")
        button3 = widgets.Button(description="Add Row")
        button4 = widgets.Button(description="Remove Row")
        button5 = widgets.Button(description="Confirm")
        button1.on_click(add_column)
        button2.on_click(remove_column)
        button3.on_click(add_row)
        button4.on_click(remove_row)
        button5.on_click(confirm)

        display(widgets.HBox([button1, button2, button3, button4]))
        draw()
        display(out)
        display(button5)

    def get_relevant_columns(self, example_columns):
        for example_column in example_columns:
            print(example_column.attr, example_column.examples)
        candidate_list = self.qbe.find_candidate_columns(example_columns)
        column_clusters = self.qbe.get_column_clusters(candidate_list)
        column_clusters = self.prepare_clusters(column_clusters)
     

        output = widgets.Output()
        global c_id
        c_id = 0
        # filter_drs = {}

        @output.capture()
        def initial_show():
            self.show_clusters(column_clusters[0], 0)

        @output.capture()
        def on_button_next(b):
            global c_id
            clear_output()
            c_id = min(len(column_clusters) - 1, c_id + 1)
            self.show_clusters(column_clusters[c_id], c_id)

        @output.capture()
        def on_button_prev(b):
            global c_id
            clear_output()
            c_id = max(0, c_id - 1)
            self.show_clusters(column_clusters[c_id], c_id)
        
        def on_button_show_views(b):
            for candidate in self.candidate_list:
                if len(candidate) == 0:
                    print("please finish selection for all columns")
            self.show_views()

        button_next = widgets.Button(description="Next")
        button_prev = widgets.Button(description="Prev")
        button_show_views = widgets.Button(description="Show Views")
        display(widgets.HBox([button_prev, button_next]))
        button_next.on_click(on_button_next)
        button_prev.on_click(on_button_prev)
        button_show_views.on_click(on_button_show_views)
        initial_show()
        display(output)
        display(button_show_views)

    def prepare_clusters(self, column_clusters_list):
        attr_clusters = []
        idx = 0
        for i, clusters in enumerate(column_clusters_list):
            clusters_list = []
            for cluster in clusters.values():
                tmp = dict()
               
                tmp["name"] = 'Column' + str(i+1)

                tmp["sample_score"] = len(cluster[0].examples_set)
              
                tmp["data"] = list(map(lambda x: (x.nid, x.tbl_name, x.attr_name, len(x.examples_set)), cluster))
              
                tmp["type"] = "object"
               
                clusters_list.append(tmp)
            sorted_list = sorted(clusters_list, key=lambda e: e.__getitem__('sample_score'), reverse=True)
            attr_clusters.append(sorted_list)
            idx += 1
        return attr_clusters

    def show_clusters(self, clusters, column_index):
        def on_button_confirm(b):
            selected_data = []
            for i in range(0, len(checkboxes)):
                if checkboxes[i].value == True:
                    selected_data.append(i)
            columns = self.clusters2Columns(clusters, selected_data)
            self.candidate_list[column_index] = columns
          
           
        def on_button_all(b):
            for checkbox in checkboxes:
                checkbox.value = True

        print(Colors.OKBLUE + "NAME: " + clusters[0]["name"] + Colors.CEND)
        checkboxes = [widgets.Checkbox(value=False, description="Cluster "+str(idx)) for idx in range(len(clusters))]
        for idx, cluster in enumerate(clusters):
            display(checkboxes[idx])
            display(HTML(tabulate(cluster["data"], headers=['id', 'Table Name', 'Attribute Name', 'Sample Score'], tablefmt='html')))
            print('\n')
        button_confirm = widgets.Button(description="Confirm")
        button_all = widgets.Button(description="Select All")
        button_confirm.on_click(on_button_confirm)
        button_all.on_click(on_button_all)
        display(widgets.HBox([button_confirm, button_all]))
     
    def clusters2Columns(self, clusters, selected_indices):
        columns = []
        for idx in selected_indices:
            for row in clusters[idx]["data"]:
                hit = self.aurum_api._nid_to_hit(int(row[0]))
                columns.append(Column(hit))
        return columns

    def show_views(self):
        init_notebook_mode(all_interactive=False)
        
        global view_id
        view_id = 0
        output = widgets.Output()

        views = []
        join_paths = self.qbe.find_joins_between_candidate_columns(self.candidate_list)
        materializer = Materializer(self.tbl_path, self.sample_size)
        for join_path in join_paths:
            view = materializer.materialize_join_path(join_path)
            views.append(view)

        @output.capture()
        def initial_show_view():
            print("view", 0)
            display(views[0])
            print(join_paths[0].to_str())

        @output.capture()
        def on_button_next_view(b):
            global view_id
            clear_output()
            view_id = min(len(views) - 1, view_id + 1)
            print("view", view_id)
            display(views[view_id])
            print(join_paths[view_id].to_str())

        @output.capture()
        def on_button_prev_view(b):
            global view_id
            clear_output()
            view_id = max(0, view_id - 1)
            print("view", view_id)
            display(views[view_id])
            print(join_paths[view_id].to_str())

        @output.capture()
        def on_button_download(b):
            global view_id
            views[view_id].to_csv(self.output_path + "view_" + str(view_id) + ".csv", encoding='latin1', index=False)
            print("Download Success!")

        button_next_2 = widgets.Button(description="Next")
        button_prev_2 = widgets.Button(description="Prev")
        download_btn = widgets.Button(description="Download")
        display(widgets.HBox([button_prev_2, button_next_2, download_btn]))
        button_next_2.on_click(on_button_next_view)
        button_prev_2.on_click(on_button_prev_view)
        download_btn.on_click(on_button_download)
        initial_show_view()
        display(output)
        print("total views:", len(views))