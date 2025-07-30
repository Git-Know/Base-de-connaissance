import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Project } from '../models/Project';

@Injectable({
  providedIn: 'root'
})
export class ProjectService {

  private apiUrl = 'http://localhost:5000/projects'; 

  constructor(private http: HttpClient) { }

  getProjects(): Observable<Project[]> {
    return this.http.get<Project[]>(this.apiUrl);
  }

  getProjectDetails(repository: string): Observable<any> {
    return this.http.get(`${this.apiUrl}/${repository}`);
  }

  getRecommendedDevelopers(repository: string) {
    return this.http.get<any[]>(`${this.apiUrl}/${repository}/recommend`);
  }

  deleteProject(repository: string): Observable<any> {
    return this.http.delete(`${this.apiUrl}/${repository}`);
  }

  addProject(formData: FormData) {
    return this.http.post(this.apiUrl, formData);
  }
}
