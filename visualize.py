#
# This script is used to visualise two datasets in the same
# plot with different colors. Used to visualize the results
# of the different tasks.
#

import pandas as pd, matplotlib.pyplot as plt, argparse

def visualize(dataPath,highlightData,samples=0,output = None):
    dataPoints = int(samples) #Collect only some of points for visualization purposes

    data = pd.read_csv(dataPath,dtype=float,header=None)
    highlight = pd.read_csv(highlightData,dtype=float,header=None)

    data['sum'] = data.sum(axis=1)
    highlight['sum'] = highlight.sum(axis=1)

    data = data.sort_values(by=['sum'],ascending=True)
    highlight = highlight.sort_values(by=['sum'],ascending=True)

    data = pd.merge(data,highlight, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
    data = data.drop(columns='sum')

    if dataPoints > 0:
        data = data.head(dataPoints)
        highlight = highlight.head(dataPoints)

    markerSize = 20
    if len(data.axes[1]) > 2:   #3D Plot
        if len(data.axes[1]) > 3: print("Only the 3 first dimensions are visualized...")
        fig = plt.figure()
        ax = fig.add_subplot(projection='3d')
        ax.scatter(data.iloc[:, 0],data.iloc[:, 1],data.iloc[:, 2],c='black',s=markerSize)
        ax.scatter(highlight.iloc[:, 0],highlight.iloc[:, 1],highlight.iloc[:, 2],c='green',s=markerSize)
    else:                       #2D Plot
        plt.scatter(data.iloc[:, 0], data.iloc[:, 1], c='black',s=markerSize)
        plt.scatter(highlight.iloc[:, 0],highlight.iloc[:, 1], c='green',s=markerSize)

    if output != None: 
        if not str(output).lower().endswith(".png"): output+=".png"
        plt.savefig(output)

    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", help = "Data to plot", default=None)
    parser.add_argument("-l", "--highlight", help = "Data to highlight", default=None )
    parser.add_argument("-s", "--samples", help = "Samples to visualise. Set 0 to use all of them.", default=300)
    parser.add_argument("-o", "--output", help = "Define where to save the plot", default=None)

    args = parser.parse_args()

    if args.data == None or args.highlight == None:
        parser.print_help()
        exit(0)
        
    visualize(args.data,args.highlight,args.samples,args.output)