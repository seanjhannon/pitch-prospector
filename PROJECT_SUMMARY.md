# Pitch Prospector - Project Summary for AI Article Generation

## Project Overview

**Pitch Prospector** is a web application that allows baseball fans, analysts, and coaches to search through millions of historical MLB at-bats to find specific pitch sequences. Think of it as "Shazam for baseball" - you can describe a sequence of pitches and outcomes, and the app will find real at-bats that match that exact pattern.

## The Problem It Solves

Baseball is a game of patterns and sequences. Coaches, players, and analysts often want to find examples of specific pitch sequences (like "fastball-strike, curveball-ball, slider-strikeout") to study how pitchers approach certain situations or how batters respond to particular patterns. Before this tool, finding such specific sequences required manually sifting through hours of game footage or extensive database queries.

## How It Works

1. **User Input**: Users select a date range and build a pitch sequence by choosing pitch types (fastball, curveball, slider, etc.) and outcomes (strike, ball, hit, etc.)

2. **Database Search**: The app searches through over 675,000+ historical at-bats in the database to find exact matches

3. **Results Display**: Matching at-bats are displayed as cards showing:
   - Pitcher and batter names with photos
   - The exact pitch sequence with speeds and locations
   - Direct links to Baseball Savant for video analysis

4. **Data Management**: The app automatically keeps data fresh by pulling new games from MLB's Statcast API

## Technical Innovation

- **Optimized Data Storage**: Uses a custom schema that stores only essential data (pitch type, outcome, speed, location) while maintaining full searchability
- **Real-time Integration**: Direct connection to MLB's Statcast API for live data
- **Smart Duplicate Prevention**: Ensures data integrity when updating with new games
- **Mobile-Responsive Design**: Works seamlessly on desktop and mobile devices

## User Value

- **For Coaches**: Find examples of successful pitch sequences to teach players
- **For Analysts**: Study patterns in how pitchers approach different situations
- **For Fans**: Discover interesting at-bats and learn about pitch sequencing
- **For Players**: Study how opponents have handled specific pitch combinations

## Data Scale

- **675,000+ at-bats** from 2015 to present
- **Millions of individual pitches** with detailed data
- **Real-time updates** as new games are played
- **Comprehensive coverage** of all MLB games

## Technology Stack

- **Frontend**: Streamlit (Python web framework)
- **Database**: Supabase PostgreSQL (cloud-hosted)
- **Data Source**: MLB Statcast API via pybaseball
- **Deployment**: Streamlit Cloud

## Key Features

- **Precise Sequence Matching**: Find at-bats with exact pitch type and outcome sequences
- **Date Range Filtering**: Search specific time periods or seasons
- **Player Integration**: See actual pitcher and batter photos and names
- **Video Links**: Direct links to Baseball Savant for pitch-by-pitch video analysis
- **Data Freshness**: One-click updates to include the latest games

## Real-World Applications

This tool bridges the gap between raw baseball data and actionable insights. Instead of needing to know SQL or have access to expensive baseball databases, anyone can now search for specific pitch sequences and find real examples to study. It democratizes access to advanced baseball analytics.

## The Inspiration

The project was inspired by a YouTube video that demonstrated the power of searching through baseball data to find specific patterns. The creator wanted to build a tool that made this kind of analysis accessible to everyone, not just data scientists or professional analysts.

## Future Potential

The application could be extended to include:
- Pitch sequence analytics and statistics
- Player-specific pattern analysis
- Situation-based searches (count, inning, score, etc.)
- Integration with other baseball data sources
- Machine learning insights about pitch sequencing effectiveness

This represents a new way of interacting with baseball data - moving from static statistics to dynamic, searchable sequences that tell the story of how the game is actually played. 