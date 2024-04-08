# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING
YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING USING SQL AND MONGODB

The "YouTube Data Harvesting and Warehousing" project is designed to enable users to access and analyze data from multiple YouTube channels. It utilizes SQL, MongoDB, and Streamlit to create a user-friendly application for retrieving, saving, and querying YouTube channel and video data.

Here are the key components and features of the project:

Components:
Streamlit: Used for creating a user-friendly interface that allows users to interact with the application for data retrieval and analysis.
Python: Primary programming language for the project, used for developing the entire application, including data retrieval, processing, analysis, and visualization.
Google API Client: Utilized to communicate with YouTube's Data API v3, enabling the retrieval of essential information such as channel details, video specifics, and comments.
MongoDB: A document database used for storing structured or unstructured data in a JSON-like format.
PostgreSQL: An advanced, highly scalable relational database management system used for storing and managing structured data with support for various data types and advanced SQL capabilities.

Ethical Perspective on YouTube Data Scraping:
Emphasizes the importance of ethical and responsible scraping practices, including adherence to YouTube's terms and conditions, obtaining appropriate authorization, and compliance with data protection regulations.
Highlights the need to handle collected data responsibly, ensuring privacy, confidentiality, and prevention of misuse or misrepresentation.
Encourages consideration of the potential impact on the platform and its community, promoting fair and sustainable scraping processes to uphold integrity while extracting valuable insights from YouTube data.

Required Libraries:
googleapiclient.discovery
streamlit
psycopg2
pymongo
pandas

Features:
Retrieval of channel and video data from YouTube using the YouTube API.
Storage of data in MongoDB as a data lake.
Migration of data from the data lake to a SQL database for efficient querying and analysis.
Search and retrieval of data from the SQL database using different search options
