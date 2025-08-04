import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms';
import { ProjectService } from '../services/project';
import { Project } from '../models/Project';
import { NgChartsModule } from 'ng2-charts';
import { ChartType } from 'chart.js';

@Component({
  standalone: true,
  selector: 'app-projects',
  imports: [
    CommonModule,
    HttpClientModule,
    FormsModule,
    NgChartsModule
  ],
  templateUrl: './projects.html',
  styleUrl: './projects.css',
  providers: [ProjectService]
})
export class Projects {
  projects: Project[] = [];
  filteredProjects: Project[] = [];
  searchTerm: string = '';
  loading = false;
  error: string | null = null;

  recommendedDevs: any[] = [];
  selectedRepo: string | null = null;
  showPopup = false;

  selectedProject: any = null;
  showDetails = false;

  pieChartLabels: string[] = [];
  pieChartData: number[] = [];
  pieChartType: ChartType = 'pie';

  barChartType: ChartType = 'bar';

  mostActiveDeveloper: string = '';

  assignmentMessage: string | null = null;
  showAssignmentPopup: boolean = false;

  projectToDelete: any = null;

  showAddPopup = false;

  newProject = {
    repository: '',
    languagesInput: '',
    frameworksInput: '',
    featuresInput: '',
    summary: ''
  };

  isEditing: boolean = false;
  editProject: any = {};
  showFullSummary = false;

  constructor(private projectService: ProjectService) {}

  ngOnInit(): void {
    this.loading = true;
    this.projectService.getProjects().subscribe({
      next: (data) => {
        this.projects = data;
        this.filteredProjects = data;
        this.loading = false;
      },
      error: (err) => {
        this.error = "Erreur lors du chargement des projets.";
        this.loading = false;
        console.error(err);
      }
    });

    this.projectService.getLanguageStats().subscribe((stats) => {
      this.pieChartLabels = Object.keys(stats);
      this.pieChartData = Object.values(stats);
    });
  }

  onSearchChange(): void {
    const term = this.searchTerm.toLowerCase().trim();

    this.filteredProjects = this.projects.filter(project =>
      project.repository.toLowerCase().includes(term) ||
      project.languages?.some(lang => lang.toLowerCase().includes(term)) ||
      project.frameworks?.some(fw => fw.toLowerCase().includes(term))
    );
  }

  openRecommendPopup(repository: string) {
    this.selectedRepo = repository;
    this.projectService.getRecommendedDevelopers(repository).subscribe({
      next: (data) => {
        this.recommendedDevs = data;
        this.showPopup = true;
      },
      error: (err) => {
        this.error = "Erreur lors du chargement des développeurs recommandés.";
        console.error(err);
      }
    });
  }

  closePopup() {
    this.showPopup = false;
    this.recommendedDevs = [];
  }

  viewDetails(repository: string) {
    this.projectService.getProjectDetails(repository).subscribe({
      next: (project) => {
        this.selectedProject = project;
        this.showDetails = true;
      },
      error: (err) => {
        console.error('Erreur chargement détails', err);
      }
    });
  }

  closeDetails() {
    this.showFullSummary = false;
    this.selectedProject = null;
    this.isEditing = false;
  }

  confirmDelete(project: any) {
    this.projectToDelete = project;
  }

  cancelDelete() {
    this.projectToDelete = null;
  }

  deleteProject(project: any): void {
    this.projectService.deleteProject(project.repository).subscribe({
      next: () => {
        this.projects = this.projects.filter(p => p.repository !== project.repository);
        this.filteredProjects = this.filteredProjects.filter(p => p.repository !== project.repository);
        this.projectToDelete = null;
      },
      error: (err) => {
        console.error('Erreur de suppression', err);
      }
    });
  }

  openAddPopup() {
    this.showAddPopup = true;
    this.newProject = {
      repository: '',
      languagesInput: '',
      frameworksInput: '',
      featuresInput: '',
      summary: ''
    };
  }

  closeAddPopup() {
    this.showAddPopup = false;
  }

  submitNewProject() {
    if (!this.newProject.repository) return;

    const languages = this.newProject.languagesInput
      ? this.newProject.languagesInput.split(',').map(s => s.trim()).filter(Boolean)
      : [];

    const frameworks = this.newProject.frameworksInput
      ? this.newProject.frameworksInput.split(',').map(s => s.trim()).filter(Boolean)
      : [];

    const features = this.newProject.featuresInput
      ? this.newProject.featuresInput.split(',').map(s => s.trim()).filter(Boolean)
      : [];

    const body = {
      repository: this.newProject.repository,
      languages,
      frameworks,
      features,
      summary: this.newProject.summary
    };

    this.projectService.addProject(body).subscribe({
      next: () => {
        this.showAddPopup = false;
        this.ngOnInit();
      },
      error: (err) => {
        console.error('Erreur ajout', err);
      }
    });
  }

  startEdit() {
    this.isEditing = true;
    this.editProject = {
      languagesInput: this.selectedProject.languages?.join(', ') || '',
      frameworksInput: this.selectedProject.frameworks?.join(', ') || '',
      featuresInput: this.selectedProject.features?.join(', ') || '',
      domainInput: this.selectedProject.domain?.join(', ') || '',
      summary: this.selectedProject.summary || ''
    };
  }

  saveEdit() {
    const updatedData = {
      languages: this.editProject.languagesInput.split(',').map((s: string) => s.trim()).filter(Boolean),
      frameworks: this.editProject.frameworksInput.split(',').map((s: string) => s.trim()).filter(Boolean),
      features: this.editProject.featuresInput.split(',').map((s: string) => s.trim()).filter(Boolean),
      domain: this.editProject.domainInput.split(',').map((s: string) => s.trim()).filter(Boolean),
      summary: this.editProject.summary
    };

    this.projectService.updateProject(this.selectedProject.repository, updatedData).subscribe({
      next: () => {
        Object.assign(this.selectedProject, updatedData);
        this.isEditing = false;
      },
      error: (err) => {
        console.error('Erreur modification', err);
      }
    });
  }

  cancelEdit() {
    this.isEditing = false;
  }

  assignDeveloper(developerName: string) {
    if (!this.selectedRepo) return;

    this.projectService.assignDeveloper(developerName, this.selectedRepo).subscribe({
      next: (res) => {
        this.assignmentMessage = res.message || "Développeur assigné !";
        this.showAssignmentPopup = true;
        this.closePopup(); // fermer popup recommandés
      },
      error: (err) => {
        this.assignmentMessage = "Échec de l'assignation.";
        this.showAssignmentPopup = true;
        console.error("Erreur lors de l'assignation :", err);
      }
    });
  }

  removeAssignedDeveloper(developerName: string) {
    if (!this.selectedProject || !this.selectedProject.repository) return;
  
    this.projectService.unassignDeveloper(developerName, this.selectedProject.repository).subscribe({
      next: (res) => {
        // Met à jour localement la liste des développeurs assignés pour mise à jour de l'affichage
        if (this.selectedProject.assigned_developers) {
          this.selectedProject.assigned_developers = this.selectedProject.assigned_developers.filter(
            (dev: any) => dev.author !== developerName
          );
        }
      },
      error: (err) => {
        console.error("Erreur lors de la désassignation :", err);
      }
    });
  }
  
}
