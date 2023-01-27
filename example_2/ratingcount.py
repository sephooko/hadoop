from mrjob.job import MRJob
import pytest
import csv
# class MRHotelRaitingCount(MRJob):
#     def mapper(self, _, line):
#         (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
#         result = [rating, 1]
#         yield result

#     def reducer(self, key, value):
#         result = [key, sum(value)]
#         yield result

# if __name__ == '__main__':
#     MRHotelRaitingCount.run()

# class MoviesRatingCount(MRJob):
#     def mapper(self, _, line):
#         (userId,movieId,rating,timestamp) = line.split(",")
#         if (rating != "rating"):
#             result = [movieId, float(rating)]
#             yield result
        
#     def reducer(self, key, value):
#         x = 0 
#         y = 0
#         for v in value:
#             x += v
#             y += 1
#             z = x / y
#             result = [key, z]
#             yield result

# if __name__ == '__main__':
#     MoviesRatingCount.run()

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
<<<<<<< HEAD
    AverageRating.run()
=======
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
>>>>>>> 32387ff85712d8be466b77e5c5e892fc09cd863a
