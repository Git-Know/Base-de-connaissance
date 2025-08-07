import { Routes } from '@angular/router';
import { Projects } from './projects/projects';
import { DevelopersComponent } from './developers/developers.component';

export const routes: Routes = [
    { path: 'projects', component: Projects },
    { path: '', redirectTo: '/projects', pathMatch: 'full' },
        { path: 'developers', component: DevelopersComponent },

  ];
