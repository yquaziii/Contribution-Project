import requests
from bs4 import BeautifulSoup
import csv
import time

def scrape_linkedin_jobs(job_roles, location):
    """
    Scrapes LinkedIn jobs based on given job roles and location.

    Args:
        job_roles (list): List of job roles to search for (e.g., ["gcp", "data engineer"]).
        location (str): Location to search in (e.g., "United Kingdom").
    """

    jobs_data = []  # List to store job information

    for job_role in job_roles:
        url = f"https://www.linkedin.com/jobs/search/?keywords={job_role}&location={location}"

        while True:
            headers = {'User-Agent': 'Mozilla/5.0'}  # Add a User-Agent
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error if the request fails

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find job listings (adjust selectors based on LinkedIn's structure)
            job_listings = soup.find_all('div', class_='base-card')

            for job in job_listings:
                title = job.find('h3', class_='base-search-card__title').text.strip()
                company = job.find('h4', class_='base-search-card__subtitle').text.strip()
                location = job.find('span', class_='job-search-card__location').text.strip()

                # Try to extract salary (if available)
                salary = None
                try:
                    salary_section = job.find('div', class_='job-search-card__salary-info')
                    salary = salary_section.text.strip()
                except AttributeError:
                    pass

                job_link_element = job.find('a', class_='base-card__full-link')
                job_link = 'https://www.linkedin.com' + job_link_element['href'] if job_link_element else None

                jobs_data.append({
                    "title": title,
                    "company": company,
                    "location": location,
                    "salary": salary,
                    "link": job_link
                })

            # Check for pagination and load the next page if available
            try:
                next_button = soup.find('button', attrs={'aria-label': 'Next'})
                next_url = 'https://www.linkedin.com' + next_button['href'] 
                url = next_url
                time.sleep(2)  # Wait before the next request
            except TypeError:
                break  # No more pages

    return jobs_data


# Example usage
job_roles = ["gcp", "data engineer"]
location = "United Kingdom"

job_data = scrape_linkedin_jobs(job_roles, location)

# Save to CSV
with open('linkedin_jobs.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['title', 'company', 'location', 'salary', 'link']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(job_data)

