import ipywidgets as widgets
from demo.kw_search import search_attribute, search_content
from IPython.display import display, clear_output
from itables import init_notebook_mode
import pandas as pd
from itables import show

class Demo:
    def __init__(self, columnInfer, viewSearch, aurum_api):
        self.columnInfer = columnInfer
        self.viewSearch = viewSearch
        self.col_values = {}
        self.filter_drs = {}
        self.aurum_api = aurum_api

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

    def start(self):
        global row_num
        global col_num
        row_num = 3
        col_num = 2
        # default_values1 = [["", ""], ['Homo sapiens', 'Doxorubicin'], ['Mus musculus', 'Ciprofloxacin']]

        # default_values1 = [["", ""],
        #                    ["Amy", "F"],
        #                    ["Ryan", "M"],
        #                    ["Gary", "M"],
        #                    ["Ken", "M"],
        #                    ["Terri", "F"]]
        #

        default_values1 = [["", ""], ["Randall's Island Park", "Carroll Park"], ["Rockaway Beach Boardwalk", "Cooper Park"]]

        default_values2 = [
            ["", "", ""],
            ["Amy", "Alberts", "European Sales Manager"],
            ["Ryan", "Cornelsen", "Production Technician - WC40"],
            ["Gary", "Altman", "Facilities Manager"]]
        attr_style = "<style>.attr input { background-color:#D0F0D0 !important; }</style>"
        x = [[widgets.Text(value=default_values1[i][j]) for j in range(col_num)] for i in range(row_num)]

        out = widgets.Output()


        @out.capture()
        def draw():
            attr_line = [widgets.HTML(attr_style)]
            attr_line.extend([item.add_class('attr') for item in x[0]])
            attrs = widgets.HBox(attr_line)
            display(widgets.VBox([attrs] + [widgets.HBox(x[i]) for i in range(1, row_num)]))


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
            for i in range(row_num):
                row = []
                for j in range(col_num):
                    if i == 0:
                        attrs.append(x[i][j].value)
                    else:
                        row.append(x[i][j].value)
                if i != 0:
                    values.append(row)
            self.get_relevant_columns()


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

    def get_relevant_columns(self):
        column_clusters = self.columnInfer.get_clusters(attrs, values, types=[])

        # col_values = {}
        for (idx, clusters) in enumerate(column_clusters):
            self.col_values[clusters[0]["name"]] = [row[idx] for row in values]

        output = widgets.Output()
        global c_id
        c_id = 0
        # filter_drs = {}

        @output.capture()
        def initial_show():
            self.columnInfer.show_clusters(column_clusters[0], self.filter_drs, self.viewSearch, 0)

        @output.capture()
        def on_button_next(b):
            global c_id
            clear_output()
            c_id = min(len(column_clusters) - 1, c_id + 1)
            self.columnInfer.show_clusters(column_clusters[c_id], self.filter_drs, self.viewSearch, c_id)

        @output.capture()
        def on_button_prev(b):
            global c_id
            clear_output()
            c_id = max(0, c_id - 1)
            self.columnInfer.show_clusters(column_clusters[c_id], self.filter_drs, self.viewSearch, c_id)
        
        def on_button_show_views(b):
            if len(self.filter_drs.keys()) < len(column_clusters):
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

    def show_views(self):
        init_notebook_mode(all_interactive=False)
        from archived.DoD import data_processing_utils as dpu

        view_metadata_mapping = dict()
        global view_id
        view_id = 0
        perf_stats = dict()
        k = 10
        output = widgets.Output()

        views = []
        for mjp, attrs_project, metadata, jp in self.viewSearch.virtual_schema_iterative_search(self.col_values, self.filter_drs,
                                                                                        perf_stats, max_hops=2,
                                                                                        debug_enumerate_all_jps=False,
                                                                                        offset=k):
            proj_view = dpu.project(mjp, attrs_project)
            views.append((proj_view, jp))

        @output.capture()
        def initial_show_view():
            print("view", 0)
            display(views[0][0])
            print(views[0][1])

        @output.capture()
        def on_button_next_view(b):
            global view_id
            clear_output()
            view_id = min(len(views) - 1, view_id + 1)
            print("view", view_id)
            display(views[view_id][0])
            print(views[view_id][1])

        @output.capture()
        def on_button_prev_view(b):
            global view_id
            clear_output()
            view_id = max(0, view_id - 1)
            print("view", view_id)
            display(views[view_id][0])
            print(views[view_id][1])

        @output.capture()
        def on_button_download(b):
            global view_id
            proj_view.to_csv(self.output_path + "view_" + str(view_id) + ".csv", encoding='latin1', index=False)
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