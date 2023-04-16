import sweetviz as sw
import pandas as pd
import sys
import seaborn as sns
import matplotlib.pyplot as plt

class analysis:
    def __init__(self,file):
        self.file=file
    def sweetplot(self):
        data=pd.read_csv(self.file)
        analyze_report=sw.analyze(data)
        analyze_report.show_html('output.html',open_browser=True)
    def seaplot(self):
        data=pd.read_csv(self.file)
        df=pd.DataFrame(data)
        sns.lineplot(x=df['detect_lat'],y=df['detect_lon'])

    def matplot(self):
        data=pd.read_csv(self.file)
        df=pd.DataFrame(data)
        plt.scatter(df['detect_lat'],df['detect_lon'])
        plt.show()

if __name__=="__main__":
    flpath=sys.argv[1]
    a=analysis(flpath)
    a.matplot()
