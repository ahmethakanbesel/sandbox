import plotly.graph_objects as go
import numpy as np

with open('grades.txt', 'r') as f:
    exam_grades = f.readlines()

# convert the strings into integers and assign them to a new list
grades = [int(grade) for grade in exam_grades]

tick_vals = ['[0, 10)', '[10, 20)', '[20, 30)', '[30, 40)', '[40, 50)', '[50, 60)', '[60, 70)', '[70, 80)', '[80, 90)',
             '[90, 100)', '[100, 110)']

for i in range(len(tick_vals)):
    tick_vals[i] = '    ' + tick_vals[i]

# Initialize a dictionary to hold the counts for each bin
histogram = [0] * 11
bins = []
# Count the scores in each bin
for s in grades:
    for b in range(0, 12):
        bin = b * 10
        bins.append(bin + 1)
        if bin <= s < bin + 10:
            histogram[b] += 1

# Create a trace for the bar plot
trace = go.Bar(
    x=bins,  # the bin edges are the x-values, but we don't want to include the rightmost edge
    y=histogram,
    text=['{} ({:.1f}%)'.format(count, count / sum(histogram) * 100) for count in histogram],
    # show count and percentage
    textposition='auto',
    hoverinfo='none',
    width=10,  # set the width of the bars
    marker={'color': bins,
            'colorscale': 'rdylgn'}
)

# Calculate the statistics to be displayed in the legend
min_score = min(grades)
max_score = max(grades)
mean = sum(grades) / len(grades)
median = np.median(grades)
mode = max(histogram)

# Create the layout for the plot
layout = go.Layout(
    xaxis=dict(
        showticklabels=True,
        tick0=0,
        nticks=10,
        dtick=10,
        tickangle=0,
        showgrid=False,
        tickmode='array',
        tickvals=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110],
        ticktext=tick_vals
    ),
    yaxis=dict(title='Count'),  # set the y-axis title
    bargap=0,  # set the gap between the bars
)

# Create a table trace for the statistics
table_trace = go.Table(
    header=dict(values=['Statistic', 'Value']),  # set the column names
    cells=dict(values=[['Minimum', 'Maximum', 'Mean', 'Median', 'Mode'], [min_score, max_score, mean, median, mode]]),
)

print(f"Min: {min_score:.2f} Max: {max_score:.2f} Mean: {mean:.2f} Median: {median:.2f}")

# Create the figure and plot
fig = go.Figure(data=[trace], layout=layout)
fig.update_layout(
    title_text="Exam Grades",
    font_family="Arial",
    font_size=18,
    title_font_family="Arial",
    title_font_color="black",
    legend_title_font_color="black"
)
fig.show()
