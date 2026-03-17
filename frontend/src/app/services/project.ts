import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Project } from '../models/Project';

@Injectable({
  providedIn: 'root'
})
export class ProjectService {

  private apiUrl = 'http://13.39.112.121:5000/projects';

  constructor(private http: HttpClient) { }

  getProjects(): Observable<Project[]> {
    return this.http.get<Project[]>(this.apiUrl);
  }

  getProjectDetails(repository: string): Observable<any> {
    return this.http.get(`${this.apiUrl}/${repository}`);
  }

  getRecommendedDevelopers(repository: string) {
    return this.http.get<any[]>(`${this.apiUrl}/${repository}/recommend/combined`);
  }

  deleteProject(repository: string): Observable<any> {
    return this.http.delete(`${this.apiUrl}/${repository}`);
  }

  addProject(project: any): Observable<any> {
    return this.http.post(`${this.apiUrl}/add`, project);
  }

  updateProject(repoName: string, projectData: any) {
    return this.http.put(`${this.apiUrl}/${repoName}`, projectData);
  }

  getLanguageStats(): Observable<{ [lang: string]: number }> {
    return this.http.get<{ [lang: string]: number }>(`${this.apiUrl}/language-stats`);
  }

  getContributionsByDeveloper() {
    return this.http.get<{ [author: string]: number }>(`${this.apiUrl}/contributors`);
  }

  assignDeveloper(developer: string, repository: string): Observable<any> {
    const body = { developer, repository };
    return this.http.post(`${this.apiUrl}/assign`, body);
  }

  unassignDeveloper(developer: string, repository: string): Observable<any> {
    const body = { developer, repository };
    return this.http.post(`${this.apiUrl}/unassign`, body);
  }

  getModulesByLanguages(technologies: string[]) {
    const body = {
      technologies: technologies
    };
    return this.http.post<any[]>(
      'http://13.39.112.121:5000/recommend/modules',
      body
    );
  }


}
