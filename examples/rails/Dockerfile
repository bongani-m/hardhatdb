FROM ruby:4.0.1-bookworm

RUN apt-get update \
	&& apt-get install -y --no-install-recommends build-essential libmariadb-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Gemfile Gemfile.lock ./
RUN bundle install
COPY . .

EXPOSE 3000
ENV RAILS_ENV=development
CMD ["bin/rails", "server", "-b", "0.0.0.0", "-p", "3000"]
