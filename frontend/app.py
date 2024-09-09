from flask import Flask, render_template
import plotly.express as px
import plotly.graph_objs as go
import json
import plotly

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def hello_world():

    models = [
        {'modelname': 'model_2024-09-09', 'MAE': '15', 'MAPE': '10%'},
        {'modelname': 'model_2024-09-08', 'MAE': '12', 'MAPE': '12%'},
        {'modelname': 'model_2024-09-07', 'MAE': '14', 'MAPE': '15%'},
        {'modelname': 'model_2024-09-06', 'MAE': '14', 'MAPE': '15%'},
        {'modelname': 'model_2024-09-05', 'MAE': '14', 'MAPE': '15%'},
        {'modelname': 'model_2024-09-04', 'MAE': '14', 'MAPE': '15%'},
        {'modelname': 'model_2024-09-03', 'MAE': '14', 'MAPE': '15%'},
        {'modelname': 'model_2024-09-02', 'MAE': '14', 'MAPE': '15%'},
        {'modelname': 'model_2024-09-01', 'MAE': '14', 'MAPE': '15%'}
    ]

    # Example data for the graph
    data = [
        go.Scatter(
            x=[1, 2, 3, 4],
            y=[10, 11, 12, 13],
            mode='lines+markers',
            name='Example'
        )
    ]
    # Convert the figure to JSON
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)



    return render_template('index.html', models=models, graphJSON=graphJSON)

if __name__ == '__main__':
    app.run(debug=True)