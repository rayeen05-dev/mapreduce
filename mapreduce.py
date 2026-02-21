from mrjob.job import MRJob 
import matplotlib.pyplot as plt 


class MRRatingCounter(MRJob): 
    def mapper(self,key,line):
        (userID,movieID,rating,timestamp) = line.split('\t')
        yield rating ,1 

    def reducer(self,rating,occurences) : 
        yield rating,sum(occurences)

if __name__=='__main__' : 
    mr_job = MRRatingCounter(args=["u.data"])
    ratings ={}
    with mr_job.make_runner() as runner : 
        runner.run()
        for rating,count in mr_job.parse_output(runner.cat_output()) : 
            ratings[rating] = count 
        
        sorted_ratings = sorted(ratings.items())
        labels = [r[0] for r in sorted_ratings] 
        counts = [r[1] for r in sorted_ratings]


        plt.bar(labels,counts,color='steelblue') 
        plt.title('movie rating distribution') 
        plt.xlabel('rating')
        plt.ylabel('count')
        plt.tight_layout()
        plt.show()