import { Routes } from '@angular/router';
import { Projects } from './projects/projects';

export const routes: Routes = [
    { path: 'projects', component: Projects },
    { path: '', redirectTo: '/projects', pathMatch: 'full' }, 
  ];
