from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        result = [rating, 1]
        yield result

    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result

if __name__ == '__main__':
    MRHotelRaitingCount.run()

class AverageRating(MRJob):

    def configure_args(self):
        super(AverageRating, self).configure_args()
        self.add_file_arg('--movies', help='movies.csv')

    def __init__(self, *args, **kwargs):
        super(AverageRating, self).__init__(*args, **kwargs)
        with open(self.options.movies, encoding="utf8") as f:
            self.movies = {row["movieId"]: row["title"] for row in csv.DictReader(f)}

    def mapper(self, _, line):
        (userId, movieId, rating, timestamp) = line.split(",")
        if rating != 'rating':
            title = self.movies.get(movieId, '')
            yield (movieId, (float(rating), 1, title))

    def reducer(self, key, value):
        ratings_sum = 0
        count = 0
        title = ""
        for rating, c, t in value:
            ratings_sum += rating
            count += c
            title = t
        avg_rating = ratings_sum/count
        yield (title, avg_rating)

if __name__ == '__main__':
    AverageRating.run()
