from pyecharts.charts import Line,Map,Pie, Geo, Grid, Page, Tab
from pyecharts import options as opts
from pyecharts.globals import ThemeType, ChartType, SymbolType
from pyecharts.commons.utils import JsCode

def line_plot(x, y, yTitle, title):
    line = Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
    line.add_xaxis(x)
    line.add_yaxis(yTitle, y, is_smooth=True)
    # 设置全局选项
    line.set_global_opts(
        title_opts=opts.TitleOpts(title=title),
        xaxis_opts=opts.AxisOpts(
            type_="time",
            is_scale=True,
            boundary_gap=False,
            axislabel_opts=opts.LabelOpts(formatter="{yyyy}-{MM}-{dd}"),
        ),
        tooltip_opts=opts.TooltipOpts(
            trigger="item",
            formatter=JsCode(
                """
                function (params) {
                    return params.seriesName + '<br/>' + 'Date: ' + params.data[0].substring(0, 10) + '<br/>' + 'count: ' + params.data[1];
                }
                """
            ),
        ),
        datazoom_opts=[opts.DataZoomOpts()],
    )
    return line

# 绘制地理分布图
def map_chart(name, count):
    c = (
        Map(init_opts=opts.InitOpts(chart_id=2, theme=ThemeType.LIGHT))
            .add("首次激活位置分布", [list(z) for z in zip(name,
                                              count)], "china")
            .set_global_opts(
            title_opts=opts.TitleOpts(title="首次激活全国的分布图",
                                      title_textstyle_opts=opts.TextStyleOpts(font_size=15,
                                                                              color='#000000')),
            visualmap_opts=opts.VisualMapOpts(max_=250),
        )
    )
    return c


def plotCityMap(cityDf):
    geo = (
        Geo()
        .add_schema(maptype="china")
        .add(
            "city",
            cityDf.values.tolist(),
            type_=ChartType.EFFECT_SCATTER,
            symbol_size=8,
        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False, formatter="{b}", font_size=8))
        .set_global_opts(
            title_opts=opts.TitleOpts(title="全国城市分布图"),
            visualmap_opts=opts.VisualMapOpts(max_=50)
            )
    )
    return geo

# 激活占比饼图
def plotPie(data, title):
    pie = (
        Pie(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
        .add(title, data, radius=["20%", "65%"])
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            legend_opts=opts.LegendOpts(is_show=True, pos_bottom="0%", pos_left="center"),
        )
        .set_series_opts(
            label_opts=opts.LabelOpts(formatter="{b}: {d}%")
        )
    )
    return pie