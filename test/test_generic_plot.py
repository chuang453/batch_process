from utils.adapters.plot_helpers import generic_plot


def test_generic_plot():
    ## 示例
    # 示例数据：list of lists
    data = [
        [[0, 10, 100], [1, 15, 105], [2, 20,
                                      110]],  # sheet0: time, temp, pressure
        [[0, 3.3], [1, 3.2], [2, 3.0]]  # sheet1: time, voltage
    ]

    def make_extractor(data):

        def extract(param):
            sheet_idx, col_idx = param
            return [row[col_idx] for row in data[sheet_idx]]

        return extract

    extract_f = make_extractor(data)  ##闭包函数

    #
    plot_spec = {
        "subplots": [
            {
                "pos": (2, 1, 1),  # 2行1列，第1个
                "title":
                "Temperature & Pressure",
                "xlabel":
                "Time (s)",
                "ylabel":
                "Value",
                "lines": [{
                    "x": (0, 0),
                    "y": [(0, 1), "Temperature", {
                        "color": "red",
                        "marker": "o"
                    }]
                }, {
                    "x": (0, 0),
                    "y": [(0, 2), "Pressure", {
                        "color": "blue",
                        "linestyle": "--"
                    }]
                }]
            },
            {
                "pos": (2, 1, 2),  # 第2个
                "title":
                "Voltage",
                "xlabel":
                "Time (s)",
                "ylabel":
                "Voltage (V)",
                "lines": [{
                    "x": (1, 0),
                    "y": [(1, 1), "Battery", {
                        "color": "green"
                    }]
                }]
            }
        ],
        "save_path":
        "multi_sensor.png"
    }

    plot_style = {
        "figsize": (8, 6),
        "grid": True,
        "tight_layout": True,
        "dpi": 200
    }

    generic_plot(extract_f, plot_spec, plot_style)
