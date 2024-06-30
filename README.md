# Exercise Badges

![](https://byob.yarr.is/sinarazi/made-template/score_ex1) ![](https://byob.yarr.is/sinarazi/made-template/score_ex2) ![](https://byob.yarr.is/sinarazi/made-template/score_ex3) ![](https://byob.yarr.is/sinarazi/made-template/score_ex4) ![](https://byob.yarr.is/sinarazi/made-template/score_ex5)

# Analysing the Impact of Air Pollution on Urban Climate in London
![Project Image](https://github.com/sinarazi/made-template/blob/main/project/pic.jpg)
Reference: Photo by [Unsplash](https://unsplash.com/photos/big-ben-london-during-daytime-uzbgq0WdgPQ)

This project examines the relationship between London's air pollution levels and weather from 2008 to
2018. The goal is to examine how changes in the weather might affect air quality by using historical weather
data along with pollution measurements. The project integrates data from the London Datastore and
Kaggle to investigate relationships between weather patterns and pollution indices, including its impact of
temperature on gasses and particle matter. Our goal is to get a deeper understanding of the environmental
dynamics in metropolitan London through this study.

## Project's tree
```
made-template/
├── .github/
│   ├── workflows/
│   │   ├── exercise-feedback.yml
│   │   └── project_test.yml
├── data/
│   ├── London_weather.sqlite
│   ├── London_pollution.sqlite
│   └── combined_London_climate.sqlite
├── examples/
├── exercises/
│   ├── exercise1.jv
│   ├── exercise2.jv
│   ├── exercise3.jv
│   ├── exercise4.jv
│   ├── exercise5.jv
├── project/
│   ├── analysis-report.ipynb
│   ├── analysis-report.pdf
│   ├── data-report.pdf
│   ├── pipeline.py
│   ├── pipeline.sh
│   ├── project-plan.md
│   ├── requirements.txt
│   ├── test_pipeline.py
│   └── tests.sh
├── .gitignore
└── README.md
```

### Analysis Summary
The project primarily focuses on the relationships between temperature, solar radiation, precipitation, and pollution levels, utilizing statistical analysis and visualization techniques to identify trends and correlations. Key findings indicate that higher temperatures and increased solar radiation tend to reduce levels of NO2 and SO2, suggesting that warmer, sunnier conditions may help dissipate these pollutants more effectively.

### Conclusions
The study underscores the significant yet intricate role weather conditions play in influencing air pollution levels. It highlights the need for comprehensive urban environmental policies that consider these dynamics to improve air quality. However, limitations such as the absence of real-time traffic data and specific emission sources are noted, pointing to areas for further research.

### Usage
To review the project findings in detail, refer to the [analysis-report.ipynb](https://github.com/sinarazi/made-template/blob/main/project/analysis-report.ipynb) or the [analysis-report.pdf](https://github.com/sinarazi/made-template/blob/main/project/analysis-report.pdf). Execute the data pipeline through pipeline.sh to reproduce the data preprocessing steps. For testing the pipeline integrity, run tests.sh.
