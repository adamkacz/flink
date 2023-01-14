from matplotlib import pyplot
from matplotlib.animation import FuncAnimation
import os
import shutil


class StatsCounter:

    def __init__(self, moving_avg_components=10, num_categories=2, plot_category="ETH-USD-sell"):
        self.num_categories = num_categories
        self.plot_category = plot_category
        self.categories = {}
        self.moving_avg_components = {}
        self.moving_average_components_limit = moving_avg_components
        self.moving_averages = {}
        self.global_averages = {}
        self.dates = {}
        self.animation = None
        self.category_plot_counts = {}

        # self.add_category(self.plot_category)

    def add_category(self, category):
        if category not in self.categories:
            path = f"plots/{category}"
            if os.path.exists(path):
                shutil.rmtree(path)

            os.mkdir(path)
            self.categories[category] = len(self.categories) + 1
            self.moving_averages[category] = []
            self.moving_avg_components[category] = []
            self.global_averages[category] = []
            self.dates[category] = []
            self.category_plot_counts[category] = 0

    def current_mean(self, category, mean, new_obs, indexes_sum, new_ind, date):
        self.add_category(category)
        self.dates[category].append(date)
        self.category_plot_counts[category] += 1
        indexes_sum_scaled = indexes_sum / self.categories[category]
        new_ind_scaled = new_ind / self.categories[category]
        new_delimiter = indexes_sum_scaled + new_ind_scaled
        new_mean = (mean * indexes_sum_scaled + new_obs) / new_delimiter
        self.global_averages[category].append(new_mean)
        self.moving_avg_components[category].append(new_obs)
        if len(self.moving_avg_components) > self.moving_average_components_limit:
            self.moving_avg_components.pop(0)
            self.moving_averages[category].append(sum(self.moving_avg_components[category])
                                                  / len(self.moving_avg_components[category]))
        else:
            self.moving_averages[category].append(None)

        # self.plot(category, date)
        return category, mean, date, new_delimiter * self.categories[category]

    def initiate_plot(self):
        figure = pyplot.figure()
        line, = pyplot.plot_date(
            self.dates[self.plot_category],
            self.moving_averages[self.plot_category],
            # list(zip(
            #     self.moving_averages[self.plot_category],
            #     self.global_averages[self.plot_category]
            # )),
            '-'
        )

        def update(frame):
            line.set_data(
                self.dates[self.plot_category],
                list(zip(
                    self.moving_averages[self.plot_category],
                    self.global_averages[self.plot_category]
                ))
            )
            figure.gca().relim()
            figure.gca().autoscale_view()
            return line,

        self.animation = FuncAnimation(figure, update, interval=200)

        pyplot.show()

    def plot(self, category, date):
        if self.category_plot_counts[category] % 50 == 0:
            pyplot.plot(self.dates[category], self.global_averages[category], label="current mean")
            pyplot.plot(self.dates[category], self.moving_averages[category], label="moving average")
            pyplot.title(f"Mean rates of: {category}")
            pyplot.legend()
            pyplot.savefig(f"plots{category}-{date}")


if __name__ == '__main__':
    pyplot.plot([1, 2, 3, 4], [None, None, 2, 3])
    pyplot.plot([1, 2, 3, 4], [5, 6, 7, 8])

    pyplot.show()
