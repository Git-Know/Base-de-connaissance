import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Developer } from '../models/Developer';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class DeveloperService {
  private apiUrl = 'http://13.39.112.121/:5000/developers';

  constructor(private http: HttpClient) { }

  getAllDevelopers(): Observable<Developer[]> {
    return this.http.get<Developer[]>(this.apiUrl);
  }
}
